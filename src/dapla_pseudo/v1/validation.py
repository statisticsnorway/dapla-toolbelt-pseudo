"""Builder for submitting a validation request."""

import asyncio
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
import polars as pl

from dapla_pseudo.utils import convert_to_date
from dapla_pseudo.utils import get_file_format_from_file_name
from dapla_pseudo.v1.client import _client
from dapla_pseudo.v1.models.api import PseudoFieldResponse
from dapla_pseudo.v1.models.api import RawPseudoMetadata
from dapla_pseudo.v1.result import Result
from dapla_pseudo.v1.supported_file_format import read_to_polars_df


class Validator:
    """Starting point for validation of datasets.

    This class should not be instantiated, only the static methods should be used.
    """

    @staticmethod
    def from_pandas(dataframe: pd.DataFrame) -> "Validator._FieldSelector":
        """Initialize a validation request from a pandas DataFrame."""
        return Validator._FieldSelector(dataframe)

    @staticmethod
    def from_polars(dataframe: pl.DataFrame) -> "Validator._FieldSelector":
        """Initialize a validation request from a polars DataFrame."""
        return Validator._FieldSelector(dataframe)

    @staticmethod
    def from_file(file_path_str: str, **kwargs: Any) -> "Validator._FieldSelector":
        """Initialize a validation request from a pandas dataframe read from file.

        Args:
            file_path_str (str): The path to the file to be read.
            kwargs (dict): Additional keyword arguments to be passed to the file reader.

        Raises:
            FileNotFoundError: If no file is found at the specified local path.

        Returns:
            _FieldSelector: An instance of the _FieldSelector class.

        Examples:
            # Read from bucket
            from dapla_pseudo import Validator
            bucket_path = "gs://ssb-staging-dapla-felles-data-delt/felles/smoke-tests/fruits/data.parquet"
            field_selector = Validator.from_file(bucket_path)

            # Read from local filesystem
            from dapla_pseudo import Validator

            local_path = "some_file.csv"
            field_selector = Validator.from_file(local_path)
        """
        file_path = Path(file_path_str)

        if not file_path.is_file() and "storage_options" not in kwargs:
            raise FileNotFoundError(f"No local file found in path: {file_path}")

        file_format = get_file_format_from_file_name(file_path)

        return Validator._FieldSelector(
            read_to_polars_df(file_format, file_path, **kwargs)
        )

    class _FieldSelector:
        """Select a field to be validated."""

        def __init__(self, dataframe: pd.DataFrame | pl.DataFrame) -> None:
            """Initialize the class."""
            self._dataframe: pl.DataFrame
            if isinstance(dataframe, pd.DataFrame):
                self._dataframe = pl.from_pandas(dataframe)
            else:
                self._dataframe = dataframe

        def on_field(self, field: str) -> "Validator._Validator":
            """Specify a single field to be validated."""
            return Validator._Validator(self._dataframe, field)

    class _Validator:
        """Assemble the validation request."""

        def __init__(
            self,
            dataframe: pl.DataFrame,
            field: str,
        ) -> None:
            self._dataframe: pl.DataFrame = dataframe
            self._field: str = field

        async def _validate_map_to_stable_id_async(
            self, sid_snapshot_date: str | date | None = None
        ) -> Result:
            """Checks if all the selected fields can be mapped to a stable ID.

            Args:
                sid_snapshot_date (Optional[str | date], optional): Date representing SID-catalogue version to use.
                    Latest if unspecified. Format: YYYY-MM-DD

            Returns:
                Result: Containing a result dataframe with associated metadata.
            """
            Validator._ensure_field_valid(self._field, self._dataframe)

            client = _client()
            all_values = self._dataframe[self._field].to_list()
            snapshot = convert_to_date(sid_snapshot_date)

            missing, extraction_time = await client._post_to_sid_endpoint(
                path="sid/lookup/batch",
                values=all_values,
                sid_snapshot_date=snapshot,
            )

            result_df = pl.Series(self._field, missing).to_frame()
            metadata_logs: list[str] = (
                [f"SID snapshot time {extraction_time}"] if extraction_time else []
            )
            return Result(
                PseudoFieldResponse(
                    data=result_df,
                    raw_metadata=[
                        RawPseudoMetadata(
                            logs=metadata_logs,
                            metrics=[],
                            datadoc=[],
                            field_name=self._field,
                        )
                    ],
                )
            )

        def validate_map_to_stable_id(
            self, sid_snapshot_date: str | date | None = None
        ) -> Result:
            """Validate mapping to SID (sync wrapper around the async batched version)."""
            return asyncio.run(self._validate_map_to_stable_id_async(sid_snapshot_date))

    @staticmethod
    def _ensure_field_valid(field: str, dataframe: pl.DataFrame) -> None:
        """Ensure that all values are numeric and valid.

        This is necessary for SID mapping.

        Args:
            field (str): The identifier field.
            dataframe (pl.DataFrame): The dataframe to validate.

        Raises:
            ValueError: If the field does not exist in the dataframe.
        """
        if field not in dataframe.columns:
            raise ValueError(f"Field '{field}' does not exist in the dataframe.")

        if dataframe.select(pl.col(field)).to_series().has_nulls():
            raise ValueError(
                f"Field '{field}' contains None/NaN values which are invalid for SID mapping."
            )

        allowed_pattern = r"^\d+$"  # only numeric
        invalid_entries = (
            dataframe.select(
                pl.col(field).str.contains(allowed_pattern).alias("is_valid"),
                pl.col(field),
            )
            .filter(~pl.col("is_valid"))
            .select(pl.col(field))
        )

        if not invalid_entries.is_empty():
            invalid_values = invalid_entries.select(pl.col(field)).to_series().to_list()
            raise ValueError(
                f"Field '{field}' contains non-numeric values which are invalid for SID mapping: {invalid_values}"
            )
