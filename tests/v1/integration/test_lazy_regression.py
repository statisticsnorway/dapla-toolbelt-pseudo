import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

import polars as pl
import pytest
from tests.v1.integration.utils import integration_test


def _build_wide_input_parquet(
    file_path: Path,
    *,
    rows: int,
    wide_columns: int,
    payload_chars: int,
) -> None:
    payload = "x" * payload_chars
    blob_columns = {
        f"blob_{idx}": [f"{idx}_{row}_{payload}" for row in range(rows)]
        for idx in range(wide_columns)
    }
    data = {
        "person_id": [f"id_{idx:09d}" for idx in range(rows)],
        **blob_columns,
    }
    pl.DataFrame(data).write_parquet(str(file_path))


def _run_case_rss_increase_bytes(
    file_path: Path,
    *,
    input_type: str,
) -> float:
    completed_process = subprocess.run(
        [
            sys.executable,
            "-m",
            "tests.v1.integration.lazy_memory_worker",
            "--input-path",
            str(file_path),
            "--input-type",
            input_type,
        ],
        capture_output=True,
        text=True,
        check=True,
    )

    stdout_lines = [
        line for line in completed_process.stdout.splitlines() if line.strip()
    ]
    payload = json.loads(stdout_lines[-1])
    return float(payload["rss_increase_bytes"])


@pytest.mark.usefixtures("setup")
@integration_test()
def test_lazy_vs_eager_memory_regression() -> None:
    """This test checks that the memory usage of pseudonymization with a LazyFrame input is significantly lower than with a DataFrame input."""
    rows = int(os.getenv("LAZY_REGRESSION_ROWS", "1000"))
    wide_columns = int(os.getenv("LAZY_REGRESSION_WIDE_COLUMNS", "100"))
    payload_chars = int(os.getenv("LAZY_REGRESSION_PAYLOAD_CHARS", "2048"))
    expected_minimum_memory_ratio = float(os.getenv("LAZY_REGRESSION_MIN_RATIO", "2.0"))

    with tempfile.TemporaryDirectory() as temp_dir:
        file_path = Path(temp_dir) / "lazy_regression_input.parquet"
        _build_wide_input_parquet(
            file_path,
            rows=rows,
            wide_columns=wide_columns,
            payload_chars=payload_chars,
        )

        dataframe_rss_increase_bytes = _run_case_rss_increase_bytes(
            file_path,
            input_type="dataframe",
        )
        lazyframe_rss_increase_bytes = _run_case_rss_increase_bytes(
            file_path,
            input_type="lazyframe",
        )

        # Regression guard: eager DataFrame input should increase RSS more than
        # LazyFrame input for the same pseudonymization field.
        memory_ratio = dataframe_rss_increase_bytes / lazyframe_rss_increase_bytes
        assert memory_ratio >= expected_minimum_memory_ratio, (
            "Expected higher RSS increase for DataFrame input than LazyFrame input, "
            f"but got ratio={memory_ratio:.2f} "
            "("
            f"dataframe={dataframe_rss_increase_bytes:.0f} bytes, "
            f"lazyframe={lazyframe_rss_increase_bytes:.0f} bytes"
            ")."
        )
