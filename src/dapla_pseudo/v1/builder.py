"""Builder for submitting a pseudonymization request."""
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
from dapla_pseudo.v1.models import Field
from dapla_pseudo.v1.models import KeyWrapper
from dapla_pseudo.v1.models import Mimetypes
from dapla_pseudo.v1.models import PseudoConfig
from dapla_pseudo.v1.models import PseudoFunction
from dapla_pseudo.v1.models import PseudoKeyset
from dapla_pseudo.v1.models import PseudonymizeFileRequest
from dapla_pseudo.v1.models import PseudoRule
from dapla_pseudo.v1.ops import _client
from dapla_pseudo.v1.ops import _dataframe_to_json
from dapla_pseudo.v1.supported_file_format import NoFileExtensionError
from dapla_pseudo.v1.supported_file_format import SupportedFileFormat


class PseudonymizationResult:
    """Holder for data and metadata returned from pseudo-service"""

    def __init__(self, df: pl.DataFrame) -> None:
        """Initialise a PseudonymizationResult."""
        self._df = df

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

        def map_to_stable_id(self) -> Self:
            self._pseudo_func = PseudoFunction(
                function_type=PseudoFunctionTypes.MAP_SID, key=PredefinedKeys.PAPIS_COMMON_KEY_1
            )
            return self

        def pseudonymize(
            self, preserve_formatting: bool = False, with_custom_function: Optional[PseudoFunction] = None
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

            self._dataframe = self._dataframe.with_columns(
                [
                    # Pseudonymize each specified column
                    pl.col(field).map(
                        lambda s, field_name=field: _do_pseudonymize_field(
                            path="pseudonymize/field",
                            field_name=field_name,
                            values=s.to_list(),
                            pseudo_func=self._pseudo_func,
                        )
                    )
                    for field in self._fields
                ]
            )
            return PseudonymizationResult(df=self._dataframe)


def _do_pseudonymize_field(
    path: str,
    field_name: str,
    values: list[str],
    pseudo_func: PseudoFunction,
    keyset: t.Optional[PseudoKeyset] = None,
) -> pl.Series:
    response: requests.Response = _client()._post_to_field_endpoint(
        path, field_name, values, pseudo_func, keyset, stream=True
    )
    return pl.Series(response.json()[0]["values"])


def _do_pseudonymize_file(
    dataframe: pd.DataFrame, fields: list[Field], pseudo_func: PseudoFunction
) -> "PseudonymizationResult":
    pseudonymize_request = PseudonymizeFileRequest(
        pseudo_config=PseudoConfig(
            rules=[
                PseudoRule(
                    name=f"{f.pattern.split('/')[-1]}-{pseudo_func}",
                    pattern=f.pattern,
                    func=str(pseudo_func),
                )
                for f in fields
            ],
            keysets=KeyWrapper(pseudo_func.key).keyset_list(),
        ),
        target_content_type=Mimetypes.JSON,
        target_uri=None,
        compression=None,
    )
    response: requests.Response = _client().pseudonymize_file(
        pseudonymize_request, _dataframe_to_json(dataframe), stream=True
    )
    return PseudonymizationResult(pandas_dataframe=pd.json_normalize(response.json()))
