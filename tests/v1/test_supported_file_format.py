from pathlib import Path
import polars as pl
import pytest
from dapla_pseudo.exceptions import ExtensionNotValidError

from dapla_pseudo.v1.supported_file_format import SupportedFileFormat, read_to_polars_df


PKG = "dapla_pseudo.v1.supported_file_format"
TEST_FILE_PATH = "tests/v1/test_files"


def test_get_pandas_function_name_unsupported_format() -> None:
    # Checks that a unsupported file extension raise a value error.
    unsupported_format = "notsupported"
    with pytest.raises(ExtensionNotValidError):
        SupportedFileFormat(unsupported_format)


@pytest.mark.parametrize("file_format", ["json", "csv", "parquet"])
def test_supported_files_read_with_polars(file_format: str) -> None:
    print(file_format)
    supported_file_format = SupportedFileFormat(file_format)
    df = read_to_polars_df(supported_file_format, Path(f"{TEST_FILE_PATH}/test.{file_format}"))
    assert isinstance(df, pl.DataFrame)


def test_unsupported_files_read_with_polars() -> None:
    xml_format = SupportedFileFormat("xml")
    with pytest.raises(ValueError):
        df = read_to_polars_df(xml_format, Path(f"{TEST_FILE_PATH}/test.xml"))
