"""Common API models for builder packages."""
from dataclasses import astuple, dataclass
from pathlib import Path
import typing as t
from typing import Optional

import pandas as pd
import polars as pl
from pydantic import BaseModel
from requests import Response
from dapla import FileClient
from dapla_pseudo.utils import get_file_format
from io import StringIO
from dapla_pseudo.v1.models import Mimetypes
from dapla_pseudo.v1.supported_file_format import (
    FORMAT_TO_MIMETYPE_FUNCTION,
    FORMAT_TO_POLARS_READER_FUNCTION,
    SupportedFileFormat,
    read_to_pandas_df,
    read_to_polars_df,
    write_from_df,
)
from dapla_pseudo.exceptions import NoFileExtensionError


@dataclass
class PseudoFileResponse:
    response: Response
    content_type: Mimetypes
    streamed: bool = True


class Result:
    """Holder for data and metadata returned from pseudo-service"""

    def __init__(
        self,
        pseudo_response: pl.DataFrame | PseudoFileResponse,
        metadata: Optional[t.Dict[str, str]] = None,
    ) -> None:
        """Initialise a PseudonymizationResult."""
        self._pseudo_response = pseudo_response
        self._metadata = metadata if metadata else {}

    def to_polars(self, **kwargs: t.Any) -> pl.DataFrame:
        """Pseudonymized Data as a Polars Dataframe."""
        match self._pseudo_response:
            case pl.DataFrame():
                return self._pseudo_response
            case PseudoFileResponse(response, content_type, _):
                format = SupportedFileFormat(content_type.name.lower())
                df = read_to_polars_df(format, StringIO(response.text), **kwargs)
                return df
            case _:
                raise ValueError(f"Invalid response type '{type(self._pseudo_response)}'")

    def to_pandas(self, **kwargs: t.Any) -> pd.DataFrame:
        """Pseudonymized Data as a Pandas Dataframe."""
        match self._pseudo_response:
            case pl.DataFrame():
                return self._pseudo_response.to_pandas()
            case PseudoFileResponse(response, content_type, _):
                format = SupportedFileFormat(content_type.name.lower())
                df = read_to_pandas_df(format, StringIO(response.text), **kwargs)
                return df
            case _:
                raise ValueError(f"Invalid response type '{type(self._pseudo_response)}'")

    def to_file(self, file_path: str, **kwargs: t.Any) -> None:
        file_format = get_file_format(file_path)

        if file_path.startswith("gs://"):
            file_handle = FileClient().gcs_open(file_path, mode="wb")
        else:
            file_handle = open(file_path, mode="wb")

        match self._pseudo_response:
            case PseudoFileResponse(response, content_type, streamed):
                if FORMAT_TO_MIMETYPE_FUNCTION[file_format] != content_type:
                    raise ValueError(
                        f'Provided output file format "{file_format}" does not match the content type of the provided input file "{content_type.name}".'
                    )
                if streamed:
                    for chunk in response.iter_content(chunk_size=128):
                        file_handle.write(chunk)
                else:
                    file_handle.write(self._pseudo_response.response.content)
            case pl.DataFrame():
                write_from_df(file_format, file_path, **kwargs)
            case _:
                raise ValueError("Invalid response type")

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
