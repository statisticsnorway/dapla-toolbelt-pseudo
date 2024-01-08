"""Common API models for builder packages."""
import typing as t
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import Optional

import pandas as pd
import polars as pl
from dapla import FileClient
from requests import Response

from dapla_pseudo.utils import get_file_format_from_file_name
from dapla_pseudo.v1.models import Mimetypes
from dapla_pseudo.v1.supported_file_format import FORMAT_TO_MIMETYPE_FUNCTION
from dapla_pseudo.v1.supported_file_format import SupportedOutputFileFormat
from dapla_pseudo.v1.supported_file_format import read_to_pandas_df
from dapla_pseudo.v1.supported_file_format import read_to_polars_df
from dapla_pseudo.v1.supported_file_format import write_from_df


@dataclass
class PseudoFileResponse:
    """PseudoFileResponse holds the data and metadata from a Pseudo Service file response."""

    response: Response
    content_type: Mimetypes
    streamed: bool = True


class Result:
    """Result represents the result of a pseudonymization operation."""

    def __init__(
        self,
        pseudo_response: pl.DataFrame | PseudoFileResponse,
        metadata: Optional[dict[str, str]] = None,
    ) -> None:
        """Initialise a PseudonymizationResult."""
        self._pseudo_response = pseudo_response
        self._metadata = metadata if metadata else {}

    def to_polars(self, **kwargs: t.Any) -> pl.DataFrame:
        """Output pseudonymized data as a Polars DataFrame.

        Args:
            **kwargs: Additional keyword arguments to be passed the Polars reader function *if* the input data is from a file.
                The specific reader function depends on the format, e.g. `read_csv` for CSV files.

        Raises:
            ValueError: If the result is not of type Polars DataFrame or PseudoFileResponse.

        Returns:
            pl.DataFrame: A Polars DataFrame containing the pseudonymized data.
        """
        match self._pseudo_response:
            case pl.DataFrame():
                return self._pseudo_response
            case PseudoFileResponse(response, content_type, _):
                output_format = SupportedOutputFileFormat(content_type.name.lower())
                df = read_to_polars_df(
                    output_format, BytesIO(response.content), **kwargs
                )
                return df
            case _:
                raise ValueError(
                    f"Invalid response type: {type(self._pseudo_response)}"
                )

    def to_pandas(self, **kwargs: t.Any) -> pd.DataFrame:
        """Output pseudonymized data as a Pandas DataFrame.

        Args:
            **kwargs: Additional keyword arguments to be passed the Pandas reader function *if* the input data is from a file.
                The specific reader function depends on the format of the input file, e.g. `read_csv()` for CSV files.

        Raises:
            ValueError: If the result is not of type Polars DataFrame or PseudoFileResponse.

        Returns:
            pd.DataFrame: A Pandas DataFrame containing the pseudonymized data.
        """
        match self._pseudo_response:
            case pl.DataFrame():
                return self._pseudo_response.to_pandas()
            case PseudoFileResponse(response, content_type, _):
                output_format = SupportedOutputFileFormat(content_type.name.lower())
                df = read_to_pandas_df(
                    output_format, BytesIO(response.content), **kwargs
                )
                return df
            case _:
                raise ValueError(
                    f"Invalid response type: {type(self._pseudo_response)}"
                )

    def to_file(self, file_path: str | Path, **kwargs: t.Any) -> None:
        """Write pseudonymized data to a file.

        Args:
            file_path (str | Path): The path to the file to be written.
            **kwargs: Additional keyword arguments to be passed the Polars writer function *if* the input data is a DataFrame.
                The specific writer function depends on the format of the output file, e.g. `write_csv()` for CSV files.

        Raises:
            ValueError: If the result is not of type Polars DataFrame or PseudoFileResponse.
            ValueError: If the output file format does not match the input file format.

        """
        file_format = get_file_format_from_file_name(file_path)

        if str(file_path).startswith("gs://"):
            file_handle = FileClient().gcs_open(file_path, mode="wb")
        else:
            file_handle = open(file_path, mode="wb")

        match self._pseudo_response:
            case PseudoFileResponse(response, content_type, streamed):
                if FORMAT_TO_MIMETYPE_FUNCTION[file_format] != content_type:
                    raise ValueError(
                        f'Provided output file format "{file_format}" does not'
                        f'match the content type of the provided input file "{content_type.name}".'
                    )
                if streamed:
                    for chunk in response.iter_content(chunk_size=128):
                        file_handle.write(chunk)
                else:
                    file_handle.write(self._pseudo_response.response.content)
            case pl.DataFrame():
                write_from_df(self._pseudo_response, file_format, file_path, **kwargs)
            case _:
                raise ValueError(
                    f"Invalid response type: {type(self._pseudo_response)}"
                )

        file_handle.close()

    @property
    def metadata(self) -> dict[str, str]:
        """Returns the pseudonymization metadata as a dictionary.

        Returns:
            Optional[dict[str, str]]: A dictionary containing the pseudonymization metadata,
            where the keys are field names and the values are corresponding pseudo field metadata.
            If no metadata is set, returns an empty dictionary.
        """
        return self._metadata
