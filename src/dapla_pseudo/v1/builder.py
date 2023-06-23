"""Builder for submitting a pseudonymization request."""
import concurrent
import typing as t
from pathlib import Path
from typing import Any
from typing import Optional

import pandas as pd
import polars as pl
import requests
from typing_extensions import Self

from dapla_pseudo.constants import PredefinedKeys
from dapla_pseudo.constants import PseudoFunctionTypes
from dapla_pseudo.v1.models import PseudoConfig
from dapla_pseudo.v1.models import PseudoFunction
from dapla_pseudo.v1.models import PseudoKeyset
from dapla_pseudo.v1.models import PseudoMetadata
from dapla_pseudo.v1.ops import _client
from dapla_pseudo.v1.supported_file_format import NoFileExtensionError
from dapla_pseudo.v1.supported_file_format import SupportedFileFormat


class PseudonymizationResult:
    """Holder for data and metadata returned from pseudo-service"""

    def __init__(self, df: pl.DataFrame, metadata: Optional[t.Dict[str, PseudoMetadata]]) -> None:
        """Initialise a PseudonymizationResult."""
        self._df = df
        self._metadata = metadata

    def to_polars(self) -> pl.DataFrame:
        """Pseudonymized Data as a Polars Dataframe."""
        return self._df

    def to_pandas(self) -> pd.DataFrame:
        """Pseudonymized Data as a Pandas Dataframe."""
        return self._df.to_pandas()


class PseudoData:
    """Starting point for pseudonymization of datasets.

    This class should not be instantiated, only the static methods should be used.
    """

    @staticmethod
    def from_pandas(dataframe: pd.DataFrame) -> "_FieldSelector":
        """Initialize a pseudonymization request from a pandas DataFrame."""
        return PseudoData._FieldSelector(dataframe)

    @staticmethod
    def from_file(file_path_str: str, **kwargs: Any) -> "_FieldSelector":
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

        pandas_function = getattr(pd, file_format.get_pandas_function_name())
        return PseudoData._FieldSelector(pandas_function(file_path_str, **kwargs))

    class _FieldSelector:
        """Select one or multiple fields to be pseudonymized."""

        def __init__(self, dataframe: pd.DataFrame):
            """Initialize the class."""
            self._dataframe = dataframe

        def on_field(self, field: str) -> "PseudoData._Pseudonymizer":
            """Specify a single field to be pseudonymized."""
            return PseudoData._Pseudonymizer(self._dataframe, [field])

        def on_fields(self, *fields: str) -> "PseudoData._Pseudonymizer":
            """Specify multiple fields to be pseudonymized."""
            return PseudoData._Pseudonymizer(self._dataframe, list(fields))

    class _Pseudonymizer:
        def __init__(self, dataframe: pd.DataFrame, fields: list[str]) -> None:
            self._dataframe: pl.DataFrame = pl.from_pandas(dataframe)
            self._fields: list[str] = fields
            self._pseudo_func: Optional[PseudoFunction] = None
            self._metadata: t.Dict[str, PseudoMetadata] = {}
            self._pseudo_keyset: Optional[PseudoKeyset] = None

        def map_to_stable_id(self) -> Self:
            self._pseudo_func = PseudoFunction(
                function_type=PseudoFunctionTypes.MAP_SID, key=PredefinedKeys.PAPIS_COMMON_KEY_1
            )
            return self

        def pseudonymize(
            self,
            preserve_formatting: bool = False,
            with_custom_function: Optional[PseudoFunction] = None,
            with_custom_keyset: Optional[PseudoKeyset] = None,
        ) -> "PseudonymizationResult":
            # If _pseudo_func has been defined upstream, then use that.
            if self._pseudo_func is None:
                # If the user has explicitly defined their own function, then use that.
                if with_custom_function is not None:
                    self._pseudo_func = with_custom_function

                # Use Format Preserving Encryption with the PAPIS compatible key (non-default case).
                elif preserve_formatting:
                    self._pseudo_func = PseudoFunction(
                        function_type=PseudoFunctionTypes.FF31,
                        key=PredefinedKeys.PAPIS_COMMON_KEY_1,
                        extra_kwargs=["strategy=SKIP"],
                    )
                # Use DAEAD with the SSB common key as a sane default.
                else:
                    self._pseudo_func = PseudoFunction(
                        function_type=PseudoFunctionTypes.DAEAD, key=PredefinedKeys.SSB_COMMON_KEY_1
                    )
            if with_custom_keyset is not None:
                self._pseudo_keyset = with_custom_keyset

            def pseudonymize_field_runner(field: str) -> tuple[str, t.Callable[[pl.Series], pl.Series]]:
                def pseudonymize_column(s: pl.Series) -> pl.Series:
                    return _do_pseudonymize_field(
                        path="pseudonymize/field",
                        field_name=field,
                        values=s.to_list(),
                        pseudo_func=self._pseudo_func,
                        metadata_map=self._metadata,
                        keyset=self._pseudo_keyset,
                    )

                return field, pseudonymize_column

            # Use concurrent.futures to execute the pseudonymization in parallel
            with concurrent.futures.ThreadPoolExecutor() as executor:
                pseudonymized_columns: t.Dict[str, t.Callable[[pl.Series], pl.Series]] = {}
                futures: t.List[concurrent.futures.Future[t.Tuple[str, t.Callable[[pl.Series], pl.Series]]]] = [
                    executor.submit(pseudonymize_field_runner, field) for field in self._fields
                ]
                for future in concurrent.futures.as_completed(futures):
                    result = future.result()
                    pseudonymized_columns[result[0]] = result[1]

            # Apply the pseudonymization to the dataframe
            self._dataframe = self._dataframe.with_columns(
                [pl.col(field).map(pseudonymized_columns[field]) for field in self._fields]
            )

            return PseudonymizationResult(df=self._dataframe, metadata=self._metadata)


def _do_pseudonymize_field(
    path: str,
    field_name: str,
    values: list[str],
    pseudo_func: Optional[PseudoFunction],
    metadata_map: t.Dict[str, PseudoMetadata],
    keyset: Optional[PseudoKeyset] = None,
) -> pl.Series:
    response: requests.Response = _client()._post_to_field_endpoint(
        path, field_name, values, pseudo_func, keyset, stream=True
    )
    pseudo_config = PseudoConfig(**response.json()["pseudoRules"])
    pseudo_metadata = PseudoMetadata(field_name=response.json()["fieldName"], pseudo_config=pseudo_config)

    metadata_map[field_name] = pseudo_metadata
    return pl.Series(response.json()["values"])
