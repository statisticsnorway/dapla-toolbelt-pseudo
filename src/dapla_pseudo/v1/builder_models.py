"""Common API models for builder packages."""
import typing as t
from typing import Optional

import pandas as pd
import polars as pl


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
