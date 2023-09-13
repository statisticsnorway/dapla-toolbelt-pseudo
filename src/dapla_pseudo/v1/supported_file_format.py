"""Classes used to support reading of dataframes from file."""
from enum import Enum
from typing import Any
from typing import Dict
from typing import Union

import pandas as pd
import polars as pl


class SupportedFileFormat(Enum):
    """Enums classes containing supported file extensions for dapla_pseudo builder."""

    CSV = "csv"
    JSON = "json"
    XML = "xml"
    PARQUET = "parquet"


FORMAT_TO_READER_FUNCTION = {
    SupportedFileFormat.CSV: pd.read_csv,
    SupportedFileFormat.JSON: pd.read_json,
    SupportedFileFormat.XML: pd.read_xml,
    SupportedFileFormat.PARQUET: pl.read_parquet,
}


def read_to_df(
    supported_format: SupportedFileFormat, file_path: str, **kwargs: Dict[str, Any]
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Reads a file with a supported file format to a Dataframe."""
    reader_function = FORMAT_TO_READER_FUNCTION[supported_format]
    return reader_function(file_path, **kwargs)


class NoFileExtensionError(Exception):
    """Exception raised when a file has no file extension."""

    def __init__(self, message: str) -> None:
        """Initialize the NoFileExtensionError."""
        super().__init__(message)
