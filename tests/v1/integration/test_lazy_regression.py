import json
import os
import statistics
import subprocess
import sys
import tempfile
from pathlib import Path

import polars as pl
import pytest
from tests.v1.integration.utils import integration_test


def _env_int(name: str, default: int) -> int:
    value = os.environ.get(name)
    if value is None:
        return default
    return int(value)


def _env_float(name: str, default: float) -> float:
    value = os.environ.get(name)
    if value is None:
        return default
    return float(value)


def _build_wide_input_parquet(
    file_path: Path,
    *,
    rows: int,
    wide_columns: int,
    payload_chars: int,
) -> None:
    payload = "x" * payload_chars
    data = {
        "person_id": [f"id_{idx:09d}" for idx in range(rows)],
        **{f"blob_{idx}": [payload] * rows for idx in range(wide_columns)},
    }
    pl.DataFrame(data).write_parquet(str(file_path))


def _run_case_peak_rss_bytes(file_path: Path, fields: list[str]) -> float:
    completed_process = subprocess.run(
        [
            sys.executable,
            "-m",
            "tests.v1.integration.lazy_memory_worker",
            "--input-path",
            str(file_path),
            "--fields",
            *fields,
        ],
        capture_output=True,
        text=True,
        check=True,
    )

    stdout_lines = [
        line for line in completed_process.stdout.splitlines() if line.strip()
    ]
    payload = json.loads(stdout_lines[-1])
    return float(payload["peak_rss_bytes"])


@pytest.mark.usefixtures("setup")
@integration_test()
def test_lazy_projection_memory_regression() -> None:
    rows = _env_int("LAZY_REGRESSION_ROWS", 1_500)
    wide_columns = _env_int("LAZY_REGRESSION_WIDE_COLUMNS", 20)
    payload_chars = _env_int("LAZY_REGRESSION_PAYLOAD_CHARS", 2_048)
    rounds = _env_int("LAZY_REGRESSION_ROUNDS", 1)
    expected_minimum_memory_ratio = _env_float("LAZY_REGRESSION_MIN_RATIO", 2.0)

    with tempfile.TemporaryDirectory() as temp_dir:
        file_path = Path(temp_dir) / "lazy_regression_input.parquet"
        _build_wide_input_parquet(
            file_path,
            rows=rows,
            wide_columns=wide_columns,
            payload_chars=payload_chars,
        )

        few_target_fields = ["person_id"]
        many_target_fields = [
            "person_id",
            *[f"blob_{idx}" for idx in range(wide_columns)],
        ]

        few_field_samples = [
            _run_case_peak_rss_bytes(file_path, few_target_fields)
            for _ in range(rounds)
        ]
        many_field_samples = [
            _run_case_peak_rss_bytes(file_path, many_target_fields)
            for _ in range(rounds)
        ]

        few_peak_rss_bytes = statistics.median(few_field_samples)
        many_peak_rss_bytes = statistics.median(many_field_samples)

        # Regression guard: targeting many wide columns should require noticeably
        # more memory than targeting a single narrow column.
        memory_ratio = many_peak_rss_bytes / few_peak_rss_bytes
        assert memory_ratio >= expected_minimum_memory_ratio, (
            "Expected higher peak RSS when pseudonymizing many target columns, "
            f"but got ratio={memory_ratio:.2f} "
            f"(few={few_peak_rss_bytes:.0f} bytes, many={many_peak_rss_bytes:.0f} bytes)."
        )
