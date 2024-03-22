"""Builder for submitting a pseudonymization request."""

import os
import typing as t
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
from datetime import date
from typing import Optional

import pandas as pd
import polars as pl

from dapla_pseudo.constants import TIMEOUT_DEFAULT
from dapla_pseudo.constants import Env
from dapla_pseudo.constants import PredefinedKeys
from dapla_pseudo.constants import PseudoFunctionTypes
from dapla_pseudo.types import FileLikeDatasetDecl
from dapla_pseudo.utils import convert_to_date
from dapla_pseudo.v1.api_models import DaeadKeywordArgs
from dapla_pseudo.v1.api_models import DepseudonymizeFileRequest
from dapla_pseudo.v1.api_models import FF31KeywordArgs
from dapla_pseudo.v1.api_models import KeyWrapper
from dapla_pseudo.v1.api_models import MapSidKeywordArgs
from dapla_pseudo.v1.api_models import Mimetypes
from dapla_pseudo.v1.api_models import PseudoConfig
from dapla_pseudo.v1.api_models import PseudoFunction
from dapla_pseudo.v1.api_models import PseudoKeyset
from dapla_pseudo.v1.api_models import PseudoRule
from dapla_pseudo.v1.client import PseudoClient
from dapla_pseudo.v1.pseudo_commons import File
from dapla_pseudo.v1.pseudo_commons import PseudoFieldResponse
from dapla_pseudo.v1.pseudo_commons import PseudoFileResponse
from dapla_pseudo.v1.pseudo_commons import RawPseudoMetadata
from dapla_pseudo.v1.pseudo_commons import get_file_data_from_dataset
from dapla_pseudo.v1.pseudo_commons import pseudo_operation_file
from dapla_pseudo.v1.pseudo_commons import pseudonymize_operation_field
from dapla_pseudo.v1.result import Result


class Depseudonymize:
    """Starting point for depseudonymization of datasets.

    This class should not be instantiated, only the static methods should be used.
    """

    dataset: File | pl.DataFrame

    @staticmethod
    def from_pandas(dataframe: pd.DataFrame) -> "Depseudonymize._Depseudonymizer":
        """Initialize a depseudonymization request from a pandas DataFrame."""
        dataset: pl.DataFrame = pl.from_pandas(dataframe)
        Depseudonymize.dataset = dataset
        return Depseudonymize._Depseudonymizer()

    @staticmethod
    def from_polars(dataframe: pl.DataFrame) -> "Depseudonymize._Depseudonymizer":
        """Initialize a depseudonymization request from a polars DataFrame."""
        Depseudonymize.dataset = dataframe
        return Depseudonymize._Depseudonymizer()

    @staticmethod
    def from_file(dataset: FileLikeDatasetDecl) -> "Depseudonymize._Depseudonymizer":
        """Initialize a depseudonymization request from a pandas dataframe read from file.

        Args:
            dataset (FileLikeDatasetDecl): Either a path to the file to be read, or a file handle.

        Returns:
            _Depseudonymizer: An instance of the _Depseudonymizer class.

        Examples:
            # Read from bucket
            from dapla import AuthClient
            from dapla_pseudo import Depseudonymize
            bucket_path = "gs://ssb-staging-dapla-felles-data-delt/felles/smoke-tests/fruits/data.parquet"
            field_selector = Depseudonymize.from_file(bucket_path)

            # Read from local filesystem
            from dapla_pseudo import Depseudonymize

            local_path = "some_file.csv"
            field_selector = Depseudonymize.from_file(local_path))
        """
        file_handle, content_type = get_file_data_from_dataset(dataset)
        Depseudonymize.dataset = File(file_handle, content_type)
        return Depseudonymize._Depseudonymizer()

    class _Depseudonymizer:
        """Select one or multiple fields to be pseudonymized."""

        def __init__(self, rules: Optional[list[PseudoRule]] = None) -> None:
            """Initialize the class."""
            self._rules: list[PseudoRule] = [] if rules is None else rules
            self._pseudo_keyset: Optional[PseudoKeyset | str] = None
            self._timeout: int = TIMEOUT_DEFAULT
            self._pseudo_client: PseudoClient = PseudoClient(
                pseudo_service_url=os.getenv(Env.PSEUDO_SERVICE_URL),
                auth_token=os.getenv(Env.PSEUDO_SERVICE_AUTH_TOKEN),
            )

        def on_fields(self, *fields: str) -> "Depseudonymize._DepseudoFuncSelector":
            """Specify one or multiple fields to be depseudonymized."""
            return Depseudonymize._DepseudoFuncSelector(list(fields), self._rules)

        def run(
            self,
            custom_keyset: Optional[PseudoKeyset | str] = None,
            timeout: int = TIMEOUT_DEFAULT,
        ) -> Result:
            """Depseudonymize the dataset.

            Args:
                custom_keyset (PseudoKeyset | str, optional): The depseudonymization keyset to use.
                    This can either be a PseudoKeyset, a JSON-string matching the fields of PseudoKeyset,
                    or a string matching one of the keys in `dapla_pseudo.constants.PredefinedKeys`. the Defaults to None.
                timeout (int): The timeout in seconds for the API call. Defaults to TIMEOUT_DEFAULT.

            Raises:
                ValueError: If no dataset has been provided, no fields have been provided, or the dataset is of an unsupported type.

            Returns:
                Result: The depseudonymized dataset and the associated metadata.
            """
            if Depseudonymize.dataset is None:
                raise ValueError("No dataset has been provided.")

            if self._rules == []:
                raise ValueError(
                    "No fields have been provided. Use the 'on_fields' method."
                )

            if custom_keyset is not None:
                self._pseudo_keyset = custom_keyset

            self._timeout = timeout
            match Depseudonymize.dataset:  # Differentiate between file and DataFrame
                case File():
                    return self._depseudonymize_file()
                case pl.DataFrame():
                    return self._depseudonymize_field()
                case _ as invalid_dataset:
                    raise ValueError(
                        f"Unsupported data type: {type(invalid_dataset)}. Should only be DataFrame or file-like type."
                    )

        def _depseudonymize_file(self) -> Result:
            """Depseudonymize the entire file."""
            # Need to type-cast explicitly. We know that Depseudonymize.dataset is a "File" if we reach this method.
            file = t.cast(File, Depseudonymize.dataset)

            depseudonymize_request = DepseudonymizeFileRequest(
                pseudo_config=PseudoConfig(
                    rules=self._rules,
                    keysets=KeyWrapper(self._pseudo_keyset).keyset_list(),
                ),
                target_content_type=Mimetypes.JSON,
                target_uri=None,
                compression=None,
            )

            pseudo_response: PseudoFileResponse = pseudo_operation_file(
                file_handle=file.file_handle,
                pseudo_operation_request=depseudonymize_request,
                input_content_type=file.content_type,
            )
            return Result(pseudo_response=pseudo_response)

        def _depseudonymize_field(self) -> Result:
            """Depseudonymizes the specified fields in the DataFrame using the provided pseudonymization function.

            The depseudonymization is performed in parallel. After the parallel processing is finished,
            the depseudonymized fields replace the original fields in the DataFrame stored in `self._dataframe`.

            Returns:
                Result: Containing the depseudonymized 'self._dataframe' and the associated metadata.
            """

            def depseudonymize_field_runner(
                field_name: str, series: pl.Series, pseudo_func: PseudoFunction
            ) -> tuple[str, pl.Series, RawPseudoMetadata]:
                """Function that performs the pseudonymization on a pandas Series.

                Args:
                    field_name (str):  The name of the field.
                    series (pl.Series): The pandas Series containing the values to be pseudonymized.
                    pseudo_func (PseudoFunction): The pseudonymization function to apply to the values.

                Returns:
                    tuple[str,pl.Series]: A tuple containing the field_name and the corresponding series.
                """
                data, metadata = pseudonymize_operation_field(
                    path="depseudonymize/field",
                    field_name=field_name,
                    values=series.to_list(),
                    pseudo_func=pseudo_func,
                    timeout=self._timeout,
                    pseudo_client=self._pseudo_client,
                    keyset=KeyWrapper(self._pseudo_keyset).keyset,
                )
                return field_name, data, metadata

            dataframe = t.cast(pl.DataFrame, Depseudonymize.dataset)
            # Execute the pseudonymization API calls in parallel
            with ThreadPoolExecutor() as executor:
                depseudonymized_field: dict[str, pl.Series] = {}
                raw_metadata_fields: list[RawPseudoMetadata] = []
                futures = [
                    executor.submit(
                        depseudonymize_field_runner,
                        rule.pattern,
                        dataframe[rule.pattern],
                        rule.func,
                    )
                    for rule in self._rules
                ]
                # Wait for the futures to finish, then add each field to pseudonymized_field map
                for future in as_completed(futures):
                    field_name, data, raw_metadata = future.result()
                    depseudonymized_field[field_name] = data
                    raw_metadata_fields.append(raw_metadata)

                depseudonymized_df = pl.DataFrame(depseudonymized_field)
                dataframe = dataframe.update(depseudonymized_df)
            return Result(
                pseudo_response=PseudoFieldResponse(
                    data=dataframe, raw_metadata=raw_metadata_fields
                )
            )

    class _DepseudoFuncSelector:
        def __init__(
            self, fields: list[str], rules: Optional[list[PseudoRule]] = None
        ) -> None:
            self._fields = fields
            self._existing_rules = [] if rules is None else rules

        def with_stable_id(
            self,
            sid_snapshot_date: Optional[str | date] = None,
            custom_key: Optional[str] = None,
        ) -> "Depseudonymize._Depseudonymizer":
            """Depseudonymize the selected fields with the default encryption algorithm (DAEAD).

            1) Decrypt stable-id
            2) Then map decrypted stable-id to fnr and return original fnr.

            Args:
                sid_snapshot_date (Optional[str | date], optional): Date representing SID-catalogue version to use.
                    Latest if unspecified. Format: YYYY-MM-DD
                custom_key (Optional[PredefinedKeys | str], optional): Override the key to use for pseudonymization.
                    Must be one of the keys defined in PredefinedKeys. If not defined, uses the default key for this function (papis-common-key-1)

            Returns:
                Self: The object configured to be mapped to fnr
            """
            kwargs = (
                MapSidKeywordArgs(
                    key_id=custom_key,
                    snapshot_date=convert_to_date(sid_snapshot_date),
                )
                if custom_key
                else MapSidKeywordArgs(snapshot_date=convert_to_date(sid_snapshot_date))
            )
            function = PseudoFunction(
                function_type=PseudoFunctionTypes.MAP_SID, kwargs=kwargs
            )
            return self._rule_constructor(function)

        def with_default_encryption(
            self, custom_key: Optional[PredefinedKeys | str] = None
        ) -> "Depseudonymize._Depseudonymizer":
            """Depseudonymize the selected fields with the default encryption algorithm (DAEAD).

            Args:
                custom_key (Optional[PredefinedKeys | str], optional): Override the key to use for pseudonymization.
                    Must be one of the keys defined in PredefinedKeys. If not defined, uses the default key for this function (ssb-common-key-1)

            Returns:
                Self: The object configured to be mapped to stable ID
            """
            kwargs = (
                DaeadKeywordArgs(key_id=custom_key)
                if custom_key
                else DaeadKeywordArgs()
            )
            function = PseudoFunction(
                function_type=PseudoFunctionTypes.DAEAD, kwargs=kwargs
            )
            return self._rule_constructor(function)

        def with_papis_compatible_encryption(
            self, custom_key: Optional[PredefinedKeys | str] = None
        ) -> "Depseudonymize._Depseudonymizer":
            """Depseudonymize the selected fields with a PAPIS-compatible encryption algorithm (FF31).

            Args:
                custom_key (Optional[PredefinedKeys | str], optional): Override the key to use for pseudonymization.
                    Must be one of the keys defined in PredefinedKeys. If not defined, uses the default key for this function (papis-common-key-1)

            Returns:
                Self: The object configured to be mapped to stable ID
            """
            kwargs = (
                FF31KeywordArgs(key_id=custom_key) if custom_key else FF31KeywordArgs()
            )
            function = PseudoFunction(
                function_type=PseudoFunctionTypes.FF31, kwargs=kwargs
            )
            return self._rule_constructor(function)

        def with_custom_function(
            self, function: PseudoFunction
        ) -> "Depseudonymize._Depseudonymizer":
            return self._rule_constructor(function)

        def _rule_constructor(
            self, func: PseudoFunction
        ) -> "Depseudonymize._Depseudonymizer":
            # If we use the depseudonymize_file endpoint, we need a glob catch-all prefix.
            rule_prefix = "**/" if isinstance(Depseudonymize.dataset, File) else ""
            rules = [
                PseudoRule(name=None, func=func, pattern=f"{rule_prefix}{field}")
                for field in self._fields
            ]
            return Depseudonymize._Depseudonymizer(self._existing_rules + rules)
