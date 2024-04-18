"""Builder for submitting a validation request."""

import json
from collections.abc import Sequence
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
import polars as pl
import requests

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
            from dapla import AuthClient
            from dapla_pseudo import Validator
            bucket_path = "gs://ssb-staging-dapla-felles-data-delt/felles/smoke-tests/fruits/data.parquet"
            storage_options = {"token": AuthClient.fetch_google_credentials()}
            field_selector = Validator.from_file(bucket_path, storage_options=storage_options)

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

        def validate_map_to_stable_id(
            self, sid_snapshot_date: str | date | None = None
        ) -> Result:
            """Checks if all the selected fields can be mapped to a stable ID.

            Args:
                sid_snapshot_date (Optional[str | date], optional): Date representing SID-catalogue version to use.
                    Latest if unspecified. Format: YYYY-MM-DD

            Returns:
                Result: Containing a result dataframe with associated metadata.
            """
            response: requests.Response = _client()._post_to_sid_endpoint(
                "sid/lookup/batch",
                self._dataframe[self._field].to_list(),
                convert_to_date(sid_snapshot_date),
                stream=True,
            )
            # The response content is received as a buffered byte stream from the server.
            # We decode the content using UTF-8, which gives us a List[Dict[str]] structure.
            result_json = json.loads(response.content.decode("utf-8"))[0]
            result: Sequence[str] = []
            metadata: list[str] = []
            if "missing" in result_json:
                result = result_json["missing"]
            if "datasetExtractionSnapshotTime" in result_json:
                extraction_time = result_json["datasetExtractionSnapshotTime"]
                metadata = [f"SID snapshot time {extraction_time}"]

            result_df = pl.Series(self._field, result).to_frame()
            # TODO - make the Validator fit the Result() class better
            return Result(
                PseudoFieldResponse(
                    data=result_df,
                    raw_metadata=[
                        RawPseudoMetadata(
                            logs=metadata,
                            metrics=[],
                            datadoc=[],
                            field_name=self._field,
                        )
                    ],
                )
            )
