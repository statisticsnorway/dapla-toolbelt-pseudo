import polars as pl
import pytest

from dapla_pseudo.v1.supported_file_format import SupportedFileFormat
from dapla_pseudo.v1.supported_file_format import read_to_df


PKG = "dapla_pseudo.v1.supported_file_format"
TEST_FILE_PATH = "tests/v1/test_files"


def test_get_pandas_function_name_unsupported_format() -> None:
    # Checks that a unsupported file extension raise a value error.
    unsupported_format = "notsupported"
    with pytest.raises(ValueError):
        SupportedFileFormat(unsupported_format)


@pytest.mark.parametrize(
    "file_format, read_with_polars",
    [
        ("json", False),
        ("csv", False),
        ("xml", False),
        ("parquet", True),
    ],
)
def test_supported_files_read_with_polars(file_format: str, read_with_polars: bool) -> None:
    supported_file_format = SupportedFileFormat(file_format)
    df = read_to_df(supported_file_format, f"{TEST_FILE_PATH}/test.{file_format}")
    assert isinstance(df, pl.DataFrame) is read_with_polars
