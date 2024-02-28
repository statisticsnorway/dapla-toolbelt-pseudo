import json
from pathlib import Path

import pandas as pd
import polars as pl
import pytest

from dapla_pseudo.exceptions import ExtensionNotValidError
from dapla_pseudo.v1.supported_file_format import SupportedOutputFileFormat
from dapla_pseudo.v1.supported_file_format import read_to_pandas_df
from dapla_pseudo.v1.supported_file_format import read_to_polars_df
from dapla_pseudo.v1.supported_file_format import write_from_df

PKG = "dapla_pseudo.v1.supported_file_format"
TEST_FILE_PATH = "tests/v1/test_files"


@pytest.fixture()
def df_polars() -> pl.DataFrame:
    with open("tests/data/personer.json") as test_data:
        return pl.from_pandas(pd.json_normalize(json.load(test_data)))


def test_get_pandas_function_name_unsupported_format() -> None:
    # Checks that a unsupported file extension raise a value error.
    unsupported_format = "notsupported"
    with pytest.raises(ExtensionNotValidError):
        SupportedOutputFileFormat(unsupported_format)


@pytest.mark.parametrize("file_format", ["json", "csv", "parquet", "xml"])
def test_read_with_pandas_supported_formats(file_format: str) -> None:
    supported_file_format = SupportedOutputFileFormat(file_format)
    df = read_to_pandas_df(
        supported_file_format, Path(f"{TEST_FILE_PATH}/test.{file_format}")
    )
    assert isinstance(df, pd.DataFrame)


@pytest.mark.parametrize("file_format", ["json", "csv", "parquet"])
def test_read_with_polars_supported_formats(file_format: str) -> None:
    supported_file_format = SupportedOutputFileFormat(file_format)
    df = read_to_polars_df(
        supported_file_format, Path(f"{TEST_FILE_PATH}/test.{file_format}")
    )

    assert isinstance(df, pl.DataFrame)


def test_read_with_polars_unsupported_xml() -> None:
    xml_format = SupportedOutputFileFormat("xml")
    with pytest.raises(ValueError):
        read_to_polars_df(xml_format, Path(f"{TEST_FILE_PATH}/test.xml"))


@pytest.mark.parametrize("file_format", ["json", "csv", "parquet", "xml"])
def test_write_from_df(
    tmp_path: Path, df_polars: pl.DataFrame, file_format: str
) -> None:
    supported_format = SupportedOutputFileFormat(file_format)

    write_from_df(df_polars, supported_format, f"{tmp_path}/test.{file_format}")
