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
    """Exception raised when a file has no file extension."""

    def __init__(self, message: str) -> None:
        """Initialize the NoFileExtensionError."""
        super().__init__(message)
