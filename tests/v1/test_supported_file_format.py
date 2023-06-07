import pandas as pd
import pytest

from dapla_pseudo.v1.supported_file_format import SupportedFileFormat


PKG = "dapla_pseudo.v1.supported_file_format"


@pytest.mark.parametrize("file_format", ["json", "csv", "xml", "parquet"])
def test_get_pandas_function_name(file_format: str) -> None:
    # Checks that a pandas function exists for all supported file formats.
    supported_file_format = SupportedFileFormat(file_format)
    assert getattr(pd, supported_file_format.get_pandas_function_name())


def test_get_pandas_function_name_unsupported_format() -> None:
    # Checks that a unsupported file extension raise a value error.
    unsupported_format = "notsupported"
    with pytest.raises(ValueError):
        SupportedFileFormat(unsupported_format)