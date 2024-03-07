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
from dapla_pseudo.v1.supported_file_format import write_from_dicts

PKG = "dapla_pseudo.v1.supported_file_format"
TEST_FILE_PATH = "tests/v1/test_files"


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
    tmp_path: Path, df_personer: pl.DataFrame, file_format: str
) -> None:
    supported_format = SupportedOutputFileFormat(file_format)
    file_handle = open(f"{tmp_path}/test.{file_format}", mode="wb")
    write_from_df(df_personer, supported_format, file_handle)


@pytest.mark.parametrize("file_format", ["json", "csv", "parquet", "xml"])
def test_write_from_dicts(
    tmp_path: Path, personer_file_path: str, file_format: str
) -> None:
    supported_format = SupportedOutputFileFormat(file_format)
    print(open(personer_file_path).read())
    file_data = json.loads(open(personer_file_path).read())
    assert isinstance(file_data, list)

    dest_path = tmp_path / f"test.{file_format}"
    write_from_dicts(file_data, supported_format, open(dest_path, mode="wb"))
