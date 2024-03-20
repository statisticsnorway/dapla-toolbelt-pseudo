import json
import typing as t
from collections.abc import Generator
from pathlib import Path

import gcsfs
import pandas as pd
import polars as pl
import pytest

from dapla_pseudo import Pseudonymize
from tests.integration.utils import integration_test
from tests.integration.utils import setup

BUCKET_GSUTIL_URI = "gs://ssb-dapla-pseudo-data-produkt-test"


@pytest.mark.parametrize(
    "output_func",
    [("bucket"), ("file"), ("pandas"), ("polars")],
)
@pytest.mark.parametrize(
    "input_func",
    [("bucket"), ("file"), ("pandas"), ("polars")],
)
@integration_test()
def test_pseudonymize_input_output_funcs(
    setup: Generator[None, None, None],
    input_func: t.Literal["file", "pandas", "polars", "bucket"],
    output_func: t.Literal["file", "pandas", "polars", "bucket"],
    tmp_path: Path,
    personer_file_path: str,
    df_personer_pandas: pd.DataFrame,
    df_personer: pl.DataFrame,
    df_personer_fnr_daead_encrypted: pl.DataFrame,
    df_pandas_personer_fnr_daead_encrypted: pd.DataFrame,
) -> None:
    """This test runs several times, once for every combination of the possible input and output datatypes.

    It is intended to end-to-end-test for the conversion between data types, e.g. Polars DataFrame -> File.
    """
    match input_func:
        case "file":
            pseudonymizer = Pseudonymize.from_file(personer_file_path)
        case "pandas":
            pseudonymizer = Pseudonymize.from_pandas(df_personer_pandas)
        case "polars":
            pseudonymizer = Pseudonymize.from_polars(df_personer)
        case "bucket":
            pseudonymizer = Pseudonymize.from_file(
                f"{BUCKET_GSUTIL_URI}/integration_tests_data/personer.json"
            )

    result = pseudonymizer.on_fields("fnr").with_default_encryption().run()

    match output_func:
        case "file":
            file_path = tmp_path / "personer_pseudo.json"
            result.to_file(str(file_path))

            expected = json.loads(
                open("tests/data/personer_pseudonymized_default_encryption.json").read()
            )
            actual = json.loads(file_path.open().read())

            assert expected == actual
        case "pandas":
            df_pandas = result.to_pandas()
            assert df_pandas_personer_fnr_daead_encrypted.equals(df_pandas)
        case "polars":
            df_polars = result.to_polars()
            assert df_personer_fnr_daead_encrypted.equals(df_polars)
        case "bucket":
            result_gsutil_uri = f"{BUCKET_GSUTIL_URI}/integration_tests_result/input_{input_func}_output_{output_func}.json"
            # Load the expected result for comparison
            expected = json.loads(
                open("tests/data/personer_pseudonymized_default_encryption.json").read()
            )
            result.to_file(result_gsutil_uri)

            fs = gcsfs.GCSFileSystem()
            with fs.open(result_gsutil_uri, "r") as f:
                # Read the json file that was written to bucket
                json_data = json.load(f)

            assert json_data == expected
