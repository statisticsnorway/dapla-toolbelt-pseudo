"""Builder for submitting a pseudonymization request."""
import concurrent
import json
import typing as t
from datetime import date
from pathlib import Path
from typing import Any
from typing import Optional

import pandas as pd
import polars as pl
import requests
from typing_extensions import Self

from dapla_pseudo.constants import PseudoFunctionTypes
from dapla_pseudo.utils import convert_to_date
from dapla_pseudo.v1.builder_models import DataFrameResult
from dapla_pseudo.v1.models import DaeadKeywordArgs
from dapla_pseudo.v1.models import FF31KeywordArgs
from dapla_pseudo.v1.models import MapSidKeywordArgs
from dapla_pseudo.v1.models import PseudoFunction
from dapla_pseudo.v1.models import PseudoKeyset
from dapla_pseudo.v1.ops import _client
from dapla_pseudo.v1.supported_file_format import NoFileExtensionError
from dapla_pseudo.v1.supported_file_format import SupportedFileFormat
from dapla_pseudo.v1.supported_file_format import read_to_df


class PseudoData:
    """Starting point for pseudonymization of datasets.

    This class should not be instantiated, only the static methods should be used.
    """

    @staticmethod
    def from_pandas(dataframe: pd.DataFrame) -> "PseudoData._FieldSelector":
        """Initialize a pseudonymization request from a pandas DataFrame."""
        return PseudoData._FieldSelector(dataframe)

    @staticmethod
    def from_polars(dataframe: pl.DataFrame) -> "PseudoData._FieldSelector":
        """Initialize a pseudonymization request from a polars DataFrame."""
        return PseudoData._FieldSelector(dataframe)

    @staticmethod
    def from_file(file_path_str: str, **kwargs: Any) -> "PseudoData._FieldSelector":
        """Initialize a pseudonymization request from a pandas dataframe read from file.

        Args:
            file_path_str (str): The path to the file to be read.
            kwargs (dict): Additional keyword arguments to be passed to the file reader.

        Raises:
            FileNotFoundError: If no file is found at the specified local path.
            NoFileExtensionError: If the file has no extension.

        Returns:
            _FieldSelector: An instance of the _FieldSelector class.

        Examples:
            # Read from bucket
            from dapla import AuthClient
            from dapla_pseudo import PseudoData
            bucket_path = "gs://ssb-staging-dapla-felles-data-delt/felles/smoke-tests/fruits/data.parquet"
            storage_options = {"token": AuthClient.fetch_google_credentials()}
            field_selector = PseudoData.from_file(bucket_path, storage_options=storage_options)

            # Read from local filesystem
            from dapla_pseudo import PseudoData

            local_path = "some_file.csv"
            field_selector = PseudoData.from_file(local_path)
        """
        file_path = Path(file_path_str)

        if not file_path.is_file() and "storage_options" not in kwargs:
            raise FileNotFoundError(f"No local file found in path: {file_path}")

        file_extension = file_path.suffix[1:]

        if file_extension == "":
            raise NoFileExtensionError(f"The file {file_path_str!r} has no file extension.")

        file_format = SupportedFileFormat(file_extension)

        return PseudoData._FieldSelector(read_to_df(file_format, file_path_str, **kwargs))

    class _FieldSelector:
        """Select one or multiple fields to be pseudonymized."""

        def __init__(self, dataframe: pd.DataFrame | pl.DataFrame):
            """Initialize the class."""
            self._dataframe: pl.DataFrame
            if isinstance(dataframe, pd.DataFrame):
                self._dataframe = pl.from_pandas(dataframe)
            else:
                self._dataframe = dataframe

        def on_field(self, field: str) -> "PseudoData._Pseudonymizer":
            """Specify a single field to be pseudonymized."""
            return PseudoData._Pseudonymizer(self._dataframe, [field])

        def on_fields(self, *fields: str) -> "PseudoData._Pseudonymizer":
            """Specify multiple fields to be pseudonymized."""
            return PseudoData._Pseudonymizer(self._dataframe, list(fields))

    class _Pseudonymizer:
        """Assemble the pseudonymization request."""

        def __init__(
            self,
            dataframe: pl.DataFrame,
            fields: list[str],
        ) -> None:
            self._dataframe: pl.DataFrame = dataframe
            self._fields: list[str] = fields
            self._pseudo_func: Optional[PseudoFunction] = None
            self._metadata: t.Dict[str, str] = {}
            self._pseudo_keyset: Optional[PseudoKeyset] = None

        def map_to_stable_id(self, sid_snapshot_date: Optional[str | date] = None) -> Self:
            """Map selected fields to stable ID.

            Args:
                sid_snapshot_date (Optional[str | date], optional): Date representing SID-catalogue version to use.
                    Latest if unspecified. Format: YYYY-MM-DD

            Returns:
                Self: The object configured to be mapped to stable ID
            """
            self._pseudo_func = PseudoFunction(
                function_type=PseudoFunctionTypes.MAP_SID,
                kwargs=MapSidKeywordArgs(snapshot_date=convert_to_date(sid_snapshot_date)),
            )
            return self

        def pseudonymize(
            self,
            preserve_formatting: bool = False,
            with_custom_function: Optional[PseudoFunction] = None,
            with_custom_keyset: Optional[PseudoKeyset] = None,
        ) -> "DataFrameResult":
            # If _pseudo_func has been defined upstream, then use that.
            if self._pseudo_func is None:
                # If the user has explicitly defined their own function, then use that.
                if with_custom_function is not None:
                    self._pseudo_func = with_custom_function

                # Use Format Preserving Encryption with the PAPIS compatible key (non-default case).
                elif preserve_formatting:
                    self._pseudo_func = PseudoFunction(
                        function_type=PseudoFunctionTypes.FF31,
                        kwargs=FF31KeywordArgs(),
                    )
                # Use DAEAD with the SSB common key as a sane default.
                else:
                    self._pseudo_func = PseudoFunction(
                        function_type=PseudoFunctionTypes.DAEAD,
                        kwargs=DaeadKeywordArgs(),
                    )
            if with_custom_keyset is not None:
                self._pseudo_keyset = with_custom_keyset

            return self._pseudonymize_field()

        def _pseudonymize_field(self) -> "DataFrameResult":
            """Pseudonymizes the specified fields in the DataFrame using the provided pseudonymization function.

            The pseudonymization is performed in parallel. After the parallel processing is finished,
            the pseudonymized fields replace the original fields in the DataFrame stored in `self._dataframe`.

            Returns:
                DataFrameResult: Containing the pseudonymized 'self._dataframe' and the associated metadata.
            """

            def pseudonymize_field_runner(field_name: str, series: pl.Series) -> tuple[str, pl.Series]:
                """Function that performs the pseudonymization on a pandas Series.

                Args:
                    series (pl.Series): The pandas Series containing the values to be pseudonymized.
                    field_name (str):  The name of the field.

                Returns:
                    tuple[str,pl.Series]: A tuple containing the field_name and the corresponding series.
                """
                return (
                    field_name,
                    _do_pseudonymize_field(
                        path="pseudonymize/field",
                        field_name=field_name,
                        values=series.to_list(),
                        pseudo_func=self._pseudo_func,
                        metadata_map=self._metadata,
                        keyset=self._pseudo_keyset,
                    ),
                )

            # Execute the pseudonymization API calls in parallel
            with concurrent.futures.ThreadPoolExecutor() as executor:
                pseudonymized_field: t.Dict[str, pl.Series] = {}
                futures = [
                    executor.submit(pseudonymize_field_runner, field, self._dataframe[field]) for field in self._fields
                ]
                # Wait for the futures to finish, then add each field to pseudonymized_field map
                for future in concurrent.futures.as_completed(futures):
                    result = future.result()
                    # Each future result contains the field_name (0) and the pseudonymize_values (1)
                    pseudonymized_field[result[0]] = result[1]

                pseudonymized_df = pl.DataFrame(pseudonymized_field)
                self._dataframe = self._dataframe.update(pseudonymized_df)

            return DataFrameResult(df=self._dataframe, metadata=self._metadata)


def _do_pseudonymize_field(
    path: str,
    field_name: str,
    values: list[str],
    pseudo_func: Optional[PseudoFunction],
    metadata_map: t.Dict[str, str],
    keyset: Optional[PseudoKeyset] = None,
) -> pl.Series:
    """Makes pseudonymization API calls for a list of values for a specific field and processes it into a polars Series.

    Args:
        path (str): The path to the pseudonymization endpoint.
        field_name (str): The name of the field being pseudonymized.
        values (list[str]): The list of values to be pseudonymized.
        pseudo_func (Optional[PseudoFunction]): The pseudonymization function to apply to the values.
        metadata_map (Dict[str, str]): A dictionary to store the metadata associated with each field.
        keyset (Optional[PseudoKeyset], optional): The pseudonymization keyset to use. Defaults to None.

    Returns:
        pl.Series: A pandas Series containing the pseudonymized values.
    """
    response: requests.Response = _client()._post_to_field_endpoint(
        path, field_name, values, pseudo_func, keyset, stream=True
    )
    metadata_map[field_name] = str(response.headers.get("metadata") or "")

    # The response content is received as a buffered byte stream from the server.
    # We decode the content using UTF-8, which gives us a List[List[str]] structure.
    # To obtain a single list of strings, we combine the values from the nested sublists into a flat list.
    nested_list = json.loads(response.content.decode("utf-8"))
    combined_list = []
    for sublist in nested_list:
        combined_list.extend(sublist)

    return pl.Series(combined_list)
