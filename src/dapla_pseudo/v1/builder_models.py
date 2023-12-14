"""Common API models for builder packages."""
import typing as t
from typing import Optional

import pandas as pd
import polars as pl
from requests import Response
from dapla import FileClient


class DataFrameResult:
    """Holder for data and metadata returned from pseudo-service"""

    def __init__(self, df: pl.DataFrame, metadata: Optional[t.Dict[str, str]] = None) -> None:
        """Initialise a PseudonymizationResult."""
        self._df = df
        self._metadata = metadata if metadata else {}

    def to_polars(self) -> pl.DataFrame:
        """Pseudonymized Data as a Polars Dataframe."""
        return self._df

    def to_pandas(self) -> pd.DataFrame:
        """Pseudonymized Data as a Pandas Dataframe."""
        return self._df.to_pandas()

    @property
    def metadata(self) -> dict[str, str]:
        """Returns the pseudonymization metadata as a dictionary.

        Returns:
            Optional[dict[str, str]]: A dictionary containing the pseudonymization metadata,
            where the keys are field names and the values are corresponding pseudo field metadata.
            If no metadata is set, returns an empty dictionary.
        """
        return self._metadata


class FileResult:
    def __init__(
        self, dataset_response: Response, metadata: Optional[t.Dict[str, str]] = None, streamed: bool = False
    ) -> None:
        """Initialise a FileResult."""
        self._dataset_response = dataset_response
        self._metadata = metadata if metadata else {}
        self._streamed = streamed

    def to_file(self, file_path: str) -> None:
        if file_path.startswith("gs://"):
            file_handle = FileClient().gcs_open(file_path, mode="wb")
        else:
            file_handle = open(file_path, mode="wb")

        if self._streamed:
            for chunk in self._dataset_response.iter_content(chunk_size=128):
                file_handle.write(chunk)
        else:
            file_handle.write(self._dataset_response.content)

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
