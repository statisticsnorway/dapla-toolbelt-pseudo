"""Builder for submitting a pseudonymization request."""

import os
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
from datetime import date

import pandas as pd
import polars as pl

from dapla_pseudo.constants import TIMEOUT_DEFAULT
from dapla_pseudo.constants import Env
from dapla_pseudo.constants import MapFailureStrategy
from dapla_pseudo.constants import PredefinedKeys
from dapla_pseudo.constants import PseudoFunctionTypes
from dapla_pseudo.types import FileLikeDatasetDecl
from dapla_pseudo.utils import convert_to_date
from dapla_pseudo.v1.api_models import DaeadKeywordArgs
from dapla_pseudo.v1.api_models import FF31KeywordArgs
from dapla_pseudo.v1.api_models import KeyWrapper
from dapla_pseudo.v1.api_models import MapSidKeywordArgs
from dapla_pseudo.v1.api_models import Mimetypes
from dapla_pseudo.v1.api_models import PseudoConfig
from dapla_pseudo.v1.api_models import PseudoFieldRequest
from dapla_pseudo.v1.api_models import PseudoFunction
from dapla_pseudo.v1.api_models import PseudoKeyset
from dapla_pseudo.v1.api_models import PseudonymizeFileRequest
from dapla_pseudo.v1.api_models import PseudoRule
from dapla_pseudo.v1.client import PseudoClient
from dapla_pseudo.v1.pseudo_commons import File
from dapla_pseudo.v1.pseudo_commons import HierarchicalDataFrame
from dapla_pseudo.v1.pseudo_commons import PseudoFieldResponse
from dapla_pseudo.v1.pseudo_commons import PseudoFileResponse
from dapla_pseudo.v1.pseudo_commons import RawPseudoMetadata
from dapla_pseudo.v1.pseudo_commons import get_file_data_from_dataset
from dapla_pseudo.v1.pseudo_commons import pseudo_operation_dataset
from dapla_pseudo.v1.pseudo_commons import pseudonymize_operation_field
from dapla_pseudo.v1.result import Result


class Pseudonymize:
    """Starting point for pseudonymization of datasets.

    This class should not be instantiated, only the static methods should be used.
    """

    dataset: File | pl.DataFrame | HierarchicalDataFrame

    @staticmethod
    def from_pandas(dataframe: pd.DataFrame) -> "Pseudonymize._Pseudonymizer":
        """Initialize a pseudonymization request from a pandas DataFrame."""
        dataset: pl.DataFrame = pl.from_pandas(dataframe)
        if pl.Struct in dataset.dtypes:
            Pseudonymize.dataset = HierarchicalDataFrame(dataset)
        else:
            Pseudonymize.dataset = dataset
        return Pseudonymize._Pseudonymizer()

    @staticmethod
    def from_polars(dataframe: pl.DataFrame) -> "Pseudonymize._Pseudonymizer":
        """Initialize a pseudonymization request from a polars DataFrame."""
        if pl.Struct in dataframe.dtypes:
            Pseudonymize.dataset = HierarchicalDataFrame(dataframe)
        else:
            Pseudonymize.dataset = dataframe
        return Pseudonymize._Pseudonymizer()

    @staticmethod
    def from_file(dataset: FileLikeDatasetDecl) -> "Pseudonymize._Pseudonymizer":
        """Initialize a pseudonymization request from a pandas dataframe read from file.

        Args:
            dataset (FileLikeDatasetDecl): Either a path to the file to be read, or a file handle.

        Returns:
            _Pseudonymizer: An instance of the _Pseudonymizer class.

        Examples:
            # Read from bucket
            from dapla import AuthClient
            from dapla_pseudo import Pseudonymize
            bucket_path = "gs://ssb-staging-dapla-felles-data-delt/felles/smoke-tests/fruits/data.parquet"
            field_selector = Pseudonymize.from_file(bucket_path)

            # Read from local filesystem
            from dapla_pseudo import Pseudonymize

            local_path = "some_file.csv"
            field_selector = Pseudonymize.from_file(local_path))
        """
        file_handle, content_type = get_file_data_from_dataset(dataset)
        Pseudonymize.dataset = File(file_handle, content_type)
        return Pseudonymize._Pseudonymizer()

    class _Pseudonymizer:
        """Select one or multiple fields to be pseudonymized."""

        def __init__(self, rules: list[PseudoRule] | None = None) -> None:
            """Initialize the class."""
            self._rules: list[PseudoRule] = [] if rules is None else rules
            self._pseudo_keyset: PseudoKeyset | str | None = None
            self._timeout: int = TIMEOUT_DEFAULT
            self._pseudo_client: PseudoClient = PseudoClient(
                pseudo_service_url=os.getenv(Env.PSEUDO_SERVICE_URL),
                auth_token=os.getenv(Env.PSEUDO_SERVICE_AUTH_TOKEN),
            )

        def on_fields(self, *fields: str) -> "Pseudonymize._PseudoFuncSelector":
            """Specify one or multiple fields to be pseudonymized."""
            return Pseudonymize._PseudoFuncSelector(list(fields), self._rules)

        def add_rules(self, rules: list[PseudoRule]) -> "Pseudonymize._Pseudonymizer":
            """Specify one or more existing pseudonymization rule."""
            return Pseudonymize._Pseudonymizer(self._rules + rules)

        def run(
            self,
            custom_keyset: PseudoKeyset | str | None = None,
            timeout: int = TIMEOUT_DEFAULT,
        ) -> Result:
            """Pseudonymize the dataset.

            Args:
                custom_keyset (PseudoKeyset, optional): The pseudonymization keyset to use. Defaults to None.
                timeout (int): The timeout in seconds for the API call. Defaults to TIMEOUT_DEFAULT.

            Raises:
                ValueError: If no dataset has been provided, no fields have been provided, or the dataset is of an unsupported type.

            Returns:
                Result: The pseudonymized dataset and the associated metadata.
            """
            if Pseudonymize.dataset is None:
                raise ValueError("No dataset has been provided.")

            if not self._rules:
                raise ValueError(
                    "No fields have been provided. Use the 'on_fields' or the 'add_rules' method."
                )

            if custom_keyset is not None:
                self._pseudo_keyset = custom_keyset

            self._timeout = timeout
            match Pseudonymize.dataset:  # Differentiate between file and DataFrame
                case File():
                    return self._pseudonymize_dataframe(Pseudonymize.dataset)
                case pl.DataFrame():
                    return self._pseudonymize_field(Pseudonymize.dataset)
                case HierarchicalDataFrame():
                    return self._pseudonymize_dataframe(Pseudonymize.dataset.contents)
                case _ as invalid_dataset:
                    raise ValueError(
                        f"Unsupported data type: {type(invalid_dataset)}. Should only be DataFrame or file-like type."
                    )

        def _pseudonymize_dataframe(self, dataframe: File | pl.DataFrame) -> Result:
            """Pseudonymize the entire dataframe."""
            pseudonymize_request = PseudonymizeFileRequest(
                pseudo_config=PseudoConfig(
                    rules=self._rules,
                    keysets=KeyWrapper(self._pseudo_keyset).keyset_list(),
                ),
                target_content_type=Mimetypes.JSON,
                target_uri=None,
                compression=None,
            )

            pseudo_response: PseudoFileResponse = pseudo_operation_dataset(
                dataset_ref=dataframe,
                pseudo_operation_request=pseudonymize_request,
            )

            return Result(pseudo_response=pseudo_response)

        def _pseudonymize_field(self, dataframe: pl.DataFrame) -> Result:
            """Pseudonymizes the specified fields in the DataFrame using the provided pseudonymization function.

            The pseudonymization is performed in parallel. After the parallel processing is finished,
            the pseudonymized fields replace the original fields in the DataFrame stored in `self._dataframe`.

            Returns:
                Result: Containing the pseudonymized 'self._dataframe' and the associated metadata.
            """

            def pseudonymize_field_runner(
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
                request = PseudoFieldRequest(
                    pseudo_func=pseudo_func,
                    keyset=KeyWrapper(self._pseudo_keyset).keyset,
                    name=field_name,
                    values=series.to_list(),
                )
                data, metadata = pseudonymize_operation_field(
                    path="pseudonymize/field",
                    pseudo_field_request=request,
                    timeout=self._timeout,
                    pseudo_client=self._pseudo_client,
                )
                return field_name, data, metadata

            # Execute the pseudonymization API calls in parallel
            with ThreadPoolExecutor() as executor:
                pseudonymized_field: dict[str, pl.Series] = {}
                raw_metadata_fields: list[RawPseudoMetadata] = []
                futures = [
                    executor.submit(
                        pseudonymize_field_runner,
                        rule.pattern,
                        dataframe[rule.pattern],
                        rule.func,
                    )
                    for rule in self._rules
                ]
                # Wait for the futures to finish, then add each field to pseudonymized_field map
                for future in as_completed(futures):
                    field_name, data, raw_metadata = future.result()
                    pseudonymized_field[field_name] = data
                    raw_metadata_fields.append(raw_metadata)

                pseudonymized_df = pl.DataFrame(pseudonymized_field)
                dataframe = dataframe.update(pseudonymized_df, include_nulls=True)
            return Result(
                pseudo_response=PseudoFieldResponse(
                    data=dataframe, raw_metadata=raw_metadata_fields
                )
            )

    class _PseudoFuncSelector:
        def __init__(
            self, fields: list[str], rules: list[PseudoRule] | None = None
        ) -> None:
            self._fields = fields
            self._existing_rules = [] if rules is None else rules

        def with_stable_id(
            self,
            sid_snapshot_date: str | date | None = None,
            custom_key: PredefinedKeys | str | None = None,
            on_map_failure: MapFailureStrategy | str | None = None,
        ) -> "Pseudonymize._Pseudonymizer":
            """Map the selected fields to Stable ID, then pseudonymize with a PAPIS-compatible encryption.

            In other words, this is a compound operation that both: 1) maps FNR to stable ID 2) then encrypts the Stable IDs.

            Args:
                sid_snapshot_date (Optional[str | date], optional): Date representing SID-catalogue version to use.
                    Latest if unspecified. Format: YYYY-MM-DD
                custom_key (Optional[PredefinedKeys | str], optional): Override the key to use for pseudonymization.
                    Must be one of the keys defined in PredefinedKeys. If not defined, uses the default key for this function (papis-common-key-1)
                on_map_failure (Optional[MapFailureStrategy | str], optional): defines how to handle mapping failures

            Returns:
                Self: The object configured to be mapped to stable ID
            """
            kwargs = (
                MapSidKeywordArgs(
                    key_id=custom_key,
                    snapshot_date=convert_to_date(sid_snapshot_date),
                    failure_strategy=(
                        None
                        if on_map_failure is None
                        else MapFailureStrategy(on_map_failure)
                    ),
                )
                if custom_key
                else MapSidKeywordArgs(
                    snapshot_date=convert_to_date(sid_snapshot_date),
                    failure_strategy=(
                        None
                        if on_map_failure is None
                        else MapFailureStrategy(on_map_failure)
                    ),
                )
            )
            function = PseudoFunction(
                function_type=PseudoFunctionTypes.MAP_SID, kwargs=kwargs
            )
            return self._rule_constructor(function)

        def with_default_encryption(
            self, custom_key: PredefinedKeys | str | None = None
        ) -> "Pseudonymize._Pseudonymizer":
            """Pseudonymize the selected fields with the default encryption algorithm (DAEAD).

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
            self, custom_key: PredefinedKeys | str | None = None
        ) -> "Pseudonymize._Pseudonymizer":
            """Pseudonymize the selected fields with a PAPIS-compatible encryption algorithm (FF31).

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
        ) -> "Pseudonymize._Pseudonymizer":
            return self._rule_constructor(function)

        def _rule_constructor(
            self, func: PseudoFunction
        ) -> "Pseudonymize._Pseudonymizer":
            # If we use the pseudonymize_file endpoint, we need a glob catch-all prefix.
            rule_prefix = "**/" if isinstance(Pseudonymize.dataset, File) else ""
            rules = [
                PseudoRule(name=None, func=func, pattern=f"{rule_prefix}{field}")
                for field in self._fields
            ]
            return Pseudonymize._Pseudonymizer(self._existing_rules + rules)
