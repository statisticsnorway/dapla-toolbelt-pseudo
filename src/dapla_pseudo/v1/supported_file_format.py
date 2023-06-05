"""Classes used to support reading of dataframes from file."""
from enum import Enum


class SupportedFileFormat(Enum):
    """Enums classes containing supported file extensions for dapla_pseudo builder."""

    CSV = "csv"
    JSON = "json"
    XML = "xml"
    PARQUET = "parquet"

    def get_pandas_function_name(self) -> str:
        """Return the pandas function name for the file format."""
        return f"read_{self.value}"


class NoFileExtensionError(Exception):
    """This error is raised when a file has no file extension."""

    def __init__(self, message: str) -> None:
        """Passes the exception to superclass."""
        super().__init__(message)
