"""Classes used to support reading of dataframes from file."""
from enum import Enum
from io import BytesIO
from pathlib import Path
from typing import Any

import pandas as pd
import polars as pl

from dapla_pseudo.exceptions import ExtensionNotValidError


class SupportedOutputFileFormat(Enum):
    """SupportedOutputFileFormat contains the supported file formats when outputting the result to a file.

    Note that this does NOT describe the valid file extensions of _input_ data when reading from a file.
    """

    CSV = "csv"
    JSON = "json"
    XML = "xml"
    PARQUET = "parquet"

    @classmethod
    def _missing_(cls, value: object) -> None:
        raise ExtensionNotValidError(
            f"{value} is not a valid file format. Valid formats: %s"
            % (", ".join([repr(m.value) for m in cls]))
        )


FORMAT_TO_MIMETYPE_FUNCTION = {
    SupportedOutputFileFormat.CSV: "text/csv",
    SupportedOutputFileFormat.JSON: "application/json",
}


def read_to_pandas_df(
    supported_format: SupportedOutputFileFormat,
    df_dataset: BytesIO | Path,
    **kwargs: Any,
) -> pd.DataFrame:
    """Reads a file with a supported file format to a Pandas Dataframe."""
    match supported_format:
        case SupportedOutputFileFormat.CSV:
            return pd.DataFrame(
                pd.read_csv(df_dataset, sep=";", **kwargs)
            )  # Pseudo Service CSV-separator is ';'
        case SupportedOutputFileFormat.JSON:
            return pd.DataFrame(pd.read_json(df_dataset, **kwargs))
        case SupportedOutputFileFormat.XML:
            return pd.read_xml(df_dataset, **kwargs)
        case SupportedOutputFileFormat.PARQUET:
            return pd.read_parquet(df_dataset, **kwargs)


def read_to_polars_df(
    supported_format: SupportedOutputFileFormat,
    df_dataset: BytesIO | Path,
    **kwargs: Any,
) -> pl.DataFrame:
    """Reads a file with a supported file format to a Polars Dataframe."""
    match supported_format:
        case SupportedOutputFileFormat.CSV:
            return pl.read_csv(
                df_dataset, separator=";", **kwargs
            )  # Pseudo Service CSV-separator is ';'
        case SupportedOutputFileFormat.JSON:
            return pl.read_json(df_dataset, **kwargs)
        case SupportedOutputFileFormat.PARQUET:
            return pl.read_parquet(df_dataset, **kwargs)
        case SupportedOutputFileFormat.XML:
            raise ValueError(
                "Unsupported file format for Polars: 'XML'. Use Pandas instead."
            )


def write_from_df(
    df: pl.DataFrame,
    supported_format: SupportedOutputFileFormat,
    file_path: Path | str,
    **kwargs: Any,
) -> None:
    """Writes to a file with a supported file format from a Dataframe."""
    match supported_format:
        case SupportedOutputFileFormat.CSV:
            df.write_csv(file=file_path, **kwargs)
        case SupportedOutputFileFormat.JSON:
            df.write_json(file=file_path, **kwargs)
        case SupportedOutputFileFormat.XML:
            df.to_pandas().to_xml(file_path, **kwargs)
        case SupportedOutputFileFormat.PARQUET:
            df.write_parquet(file_path, **kwargs)
