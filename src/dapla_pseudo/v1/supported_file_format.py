"""Classes used to support reading of dataframes from file."""
from enum import Enum
from functools import partial
from io import StringIO
from pathlib import Path
from typing import Any, Callable, cast
from typing import Dict
from typing import Union

import pandas as pd
import polars as pl

from dapla_pseudo.exceptions import ExtensionNotValidError


class SupportedFileFormat(Enum):
    """Enums classes containing supported file extensions for dapla_pseudo builder."""

    CSV = "csv"
    JSON = "json"
    XML = "xml"
    PARQUET = "parquet"

    @classmethod
    def _missing_(cls, value):
        raise ExtensionNotValidError(
            f"{value} is not a valid file format. Valid formats: %s" % (", ".join([repr(m.value) for m in cls]))
        )


FORMAT_TO_MIMETYPE_FUNCTION = {
    SupportedFileFormat.CSV: "text/csv",
    SupportedFileFormat.JSON: "application/json",
}

FORMAT_TO_POLARS_READER_FUNCTION: Dict[SupportedFileFormat, Callable[..., pl.DataFrame]] = {
    SupportedFileFormat.CSV: lambda source, **kwargs: pl.read_csv(
        source, separator=";", **kwargs
    ),  # Pseudo Service separator is ';'
    SupportedFileFormat.JSON: pl.read_json,
    SupportedFileFormat.PARQUET: pl.read_parquet,
}

FORMAT_TO_PANDAS_READER_FUNCTION: Dict[SupportedFileFormat, Callable[..., pd.DataFrame]] = {
    SupportedFileFormat.CSV: lambda source, **kwargs: pd.read_csv(
        source, sep=";", **kwargs
    ),  # Pseudo Service separator is ';'
    SupportedFileFormat.JSON: pd.read_json,
    SupportedFileFormat.XML: pd.read_xml,
    SupportedFileFormat.PARQUET: pd.read_parquet,
}

FORMAT_TO_WRITER_FUNCTION: Dict[SupportedFileFormat, Callable[..., None]] = {
    SupportedFileFormat.CSV: pd.DataFrame.to_csv,
    SupportedFileFormat.JSON: pd.DataFrame.to_json,
    SupportedFileFormat.XML: pd.DataFrame.to_xml,
    SupportedFileFormat.PARQUET: pl.DataFrame.write_parquet,
}


def read_to_pandas_df(
    supported_format: SupportedFileFormat, file_path: Path | StringIO, **kwargs: Dict[str, Any]
) -> pd.DataFrame:
    """Reads a file with a supported file format to a Dataframe."""
    try:
        reader_function = FORMAT_TO_POLARS_READER_FUNCTION[supported_format]
    except KeyError:
        raise ValueError(f"Unsupported file format for Pandas: {supported_format}")
    df = reader_function(file_path, **kwargs)
    assert isinstance(df, pd.DataFrame)
    return df


def read_to_polars_df(
    supported_format: SupportedFileFormat, file_path: Path | StringIO, **kwargs: Dict[str, Any]
) -> pl.DataFrame:
    """Reads a file with a supported file format to a Dataframe."""
    try:
        reader_function = FORMAT_TO_POLARS_READER_FUNCTION[supported_format]
    except KeyError:
        raise ValueError(f"Unsupported file format for Polars: {supported_format}")
    df = reader_function(file_path, **kwargs)
    assert isinstance(df, pl.DataFrame)
    return df


def write_from_df(supported_format: SupportedFileFormat, file_path: Path | str, **kwargs: Dict[str, Any]) -> None:
    """Writes a file with a supported file format to a Dataframe."""
    writer_function = FORMAT_TO_WRITER_FUNCTION[supported_format]
    return writer_function(file_path, **kwargs)
