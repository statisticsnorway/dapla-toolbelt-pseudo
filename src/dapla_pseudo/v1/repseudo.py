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
from dapla_pseudo.types import FileLikeDatasetDecl
from dapla_pseudo.v1.api_models import KeyWrapper
from dapla_pseudo.v1.api_models import Mimetypes
from dapla_pseudo.v1.api_models import PseudoConfig
from dapla_pseudo.v1.api_models import PseudoFunction
from dapla_pseudo.v1.api_models import PseudoKeyset
from dapla_pseudo.v1.api_models import PseudoRule
from dapla_pseudo.v1.api_models import RepseudoFieldRequest
from dapla_pseudo.v1.api_models import RepseudonymizeFileRequest
from dapla_pseudo.v1.baseclass import _RuleConstructor
from dapla_pseudo.v1.client import PseudoClient
from dapla_pseudo.v1.pseudo_commons import File
from dapla_pseudo.v1.pseudo_commons import PseudoFieldResponse
from dapla_pseudo.v1.pseudo_commons import PseudoFileResponse
from dapla_pseudo.v1.pseudo_commons import RawPseudoMetadata
from dapla_pseudo.v1.pseudo_commons import get_file_data_from_dataset
from dapla_pseudo.v1.pseudo_commons import pseudo_operation_file
from dapla_pseudo.v1.pseudo_commons import pseudonymize_operation_field
from dapla_pseudo.v1.result import Result


class Repseudonymize:
    """Starting point for pseudonymization of datasets.

    This class should not be instantiated, only the static methods should be used.
    """

    dataset: File | pl.DataFrame

    @staticmethod
    def from_pandas(dataframe: pd.DataFrame) -> "Repseudonymize._Repseudonymizer":
        """Initialize a pseudonymization request from a pandas DataFrame."""
        dataset: pl.DataFrame = pl.from_pandas(dataframe)
        Repseudonymize.dataset = dataset
        return Repseudonymize._Repseudonymizer()

    @staticmethod
    def from_polars(dataframe: pl.DataFrame) -> "Repseudonymize._Repseudonymizer":
        """Initialize a pseudonymization request from a polars DataFrame."""
        Repseudonymize.dataset = dataframe
        return Repseudonymize._Repseudonymizer()

    @staticmethod
    def from_file(dataset: FileLikeDatasetDecl) -> "Repseudonymize._Repseudonymizer":
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
        Repseudonymize.dataset = File(file_handle, content_type)
        return Repseudonymize._Repseudonymizer()

    class _Repseudonymizer:
        """Select one or multiple fields to be pseudonymized."""

        source_rules: t.ClassVar[list[PseudoRule]] = []
        target_rules: t.ClassVar[list[PseudoRule]] = []

        def __init__(
            self,
            source_rules: t.Optional[list[PseudoRule]] = None,
            target_rules: t.Optional[list[PseudoRule]] = None,
        ) -> None:
            """Initialize the class."""
            if source_rules is None or target_rules is None:
                # Because the "source_rules" and "target_rules" are static class variables,
                # we need to reset the lists when the first call to "_Repseudonymizer" is made.
                # This is because if we were to use the base class "Repseudonymize" twice in the same file,
                # the lists of rules would persist across runs.
                # *This is very hacky*, but the alternative is to pass the rules through
                # "_RepseudoFuncSelectorSource" and "_RepseudoFuncSelectorTarget", which would hurt readability
                Repseudonymize._Repseudonymizer.source_rules = []
                Repseudonymize._Repseudonymizer.target_rules = []
            else:
                Repseudonymize._Repseudonymizer.source_rules.extend(source_rules)
                Repseudonymize._Repseudonymizer.target_rules.extend(target_rules)

            self._source_pseudo_keyset: Optional[PseudoKeyset | str] = None
            self._target_pseudo_keyset: Optional[PseudoKeyset | str] = None
            self._timeout: int
            self._pseudo_client: PseudoClient = PseudoClient(
                pseudo_service_url=os.getenv(Env.PSEUDO_SERVICE_URL),
                auth_token=os.getenv(Env.PSEUDO_SERVICE_AUTH_TOKEN),
            )

        def on_fields(
            self, *fields: str
        ) -> "Repseudonymize._RepseudoFuncSelectorSource":
            """Specify one or multiple fields to be pseudonymized."""
            return Repseudonymize._RepseudoFuncSelectorSource(list(fields))

        def run(
            self,
            custom_source_keyset: Optional[PseudoKeyset | str] = None,
            custom_target_keyset: Optional[PseudoKeyset | str] = None,
            timeout: int = TIMEOUT_DEFAULT,
        ) -> Result:
            """Pseudonymize the dataset.

            Args:
                custom_source_keyset (PseudoKeyset, optional): The source pseudonymization keyset to use. Defaults to None.
                custom_target_keyset (PseudoKeyset, optional): The target pseudonymization keyset to use. Defaults to None.
                timeout (int): The timeout in seconds for the API call. Defaults to TIMEOUT_DEFAULT.

            Raises:
                ValueError: If no dataset has been provided, no fields have been provided, or the dataset is of an unsupported type.

            Returns:
                Result: The pseudonymized dataset and the associated metadata.
            """
            if Repseudonymize.dataset is None:
                raise ValueError("No dataset has been provided.")

            if (
                Repseudonymize._Repseudonymizer.source_rules == []
                or Repseudonymize._Repseudonymizer.target_rules == []
            ):
                raise ValueError(
                    "No fields have been provided. Use the 'on_fields' method."
                )

            if custom_source_keyset is not None:
                self._source_pseudo_keyset = custom_source_keyset

            if custom_target_keyset is not None:
                self._target_pseudo_keyset = custom_target_keyset

            self._timeout = timeout
            match Repseudonymize.dataset:  # Differentiate between file and DataFrame
                case File():
                    return self._repseudonymize_file()
                case pl.DataFrame():
                    return self._repseudonymize_field()
                case _ as invalid_dataset:
                    raise ValueError(
                        f"Unsupported data type: {type(invalid_dataset)}. Should only be DataFrame or file-like type."
                    )

        def _repseudonymize_file(self) -> Result:
            """Pseudonymize the entire file."""
            # Need to type-cast explicitly. We know that Pseudonymize.dataset is a "File" if we reach this method.
            file = t.cast(File, Repseudonymize.dataset)

            pseudonymize_request = RepseudonymizeFileRequest(
                source_pseudo_config=PseudoConfig(
                    rules=self.source_rules,
                    keysets=KeyWrapper(self._source_pseudo_keyset).keyset_list(),
                ),
                target_pseudo_config=PseudoConfig(
                    rules=self.target_rules,
                    keysets=KeyWrapper(self._target_pseudo_keyset).keyset_list(),
                ),
                target_content_type=Mimetypes.JSON,
                target_uri=None,
                compression=None,
            )

            pseudo_response: PseudoFileResponse = pseudo_operation_file(
                file_handle=file.file_handle,
                pseudo_operation_request=pseudonymize_request,
                input_content_type=file.content_type,
            )

            return Result(pseudo_response=pseudo_response)

        def _repseudonymize_field(self) -> Result:
            """Pseudonymizes the specified fields in the DataFrame using the provided pseudonymization function.

            The pseudonymization is performed in parallel. After the parallel processing is finished,
            the pseudonymized fields replace the original fields in the DataFrame stored in `self._dataframe`.

            Returns:
                Result: Containing the pseudonymized 'self._dataframe' and the associated metadata.
            """

            def repseudonymize_field_runner(
                field_name: str,
                series: pl.Series,
                source_pseudo_func: PseudoFunction,
                target_pseudo_func: PseudoFunction,
            ) -> tuple[str, pl.Series, RawPseudoMetadata]:
                """Function that performs the pseudonymization on a pandas Series.

                Args:
                    field_name (str):  The name of the field.
                    series (pl.Series): The pandas Series containing the values to be pseudonymized.
                    source_pseudo_func (PseudoFunction): The Pseudo function previously used to pseudonymize the dataset.
                    target_pseudo_func (PseudoFunction): The Pseudo function to apply to the dataset.

                Returns:
                    tuple[str,pl.Series]: A tuple containing the field_name and the corresponding series.
                """
                request = RepseudoFieldRequest(
                    source_pseudo_func=source_pseudo_func,
                    target_pseudo_func=target_pseudo_func,
                    source_keyset=KeyWrapper(self._source_pseudo_keyset).keyset,
                    target_keyset=KeyWrapper(self._target_pseudo_keyset).keyset,
                    name=field_name,
                    values=series.to_list(),
                )
                data, metadata = pseudonymize_operation_field(
                    path="repseudonymize/field",
                    pseudo_field_request=request,
                    timeout=self._timeout,
                    pseudo_client=self._pseudo_client,
                )
                return field_name, data, metadata

            dataframe = t.cast(pl.DataFrame, Repseudonymize.dataset)
            # Execute the pseudonymization API calls in parallel
            with ThreadPoolExecutor() as executor:
                pseudonymized_field: dict[str, pl.Series] = {}
                raw_metadata_fields: list[RawPseudoMetadata] = []
                futures = [
                    executor.submit(
                        repseudonymize_field_runner,
                        source_rule.pattern,
                        dataframe[source_rule.pattern],
                        source_rule.func,
                        target_rule.func,
                    )
                    for (source_rule, target_rule) in zip(
                        self.source_rules, self.target_rules
                    )
                ]
                # Wait for the futures to finish, then add each field to pseudonymized_field map
                for future in as_completed(futures):
                    field_name, data, raw_metadata = future.result()
                    pseudonymized_field[field_name] = data
                    raw_metadata_fields.append(raw_metadata)

                pseudonymized_df = pl.DataFrame(pseudonymized_field)
                dataframe = dataframe.update(pseudonymized_df)
            return Result(
                pseudo_response=PseudoFieldResponse(
                    data=dataframe, raw_metadata=raw_metadata_fields
                )
            )

    class _RepseudoFuncSelectorSource(_RuleConstructor):
        def __init__(self, fields: list[str]):
            dataset_type: t.Literal["dataframe", "file"] = (
                "dataframe"
                if isinstance(Repseudonymize.dataset, pl.DataFrame)
                else "file"
            )
            self.fields = fields
            super().__init__(fields, dataset_type)

        def from_stable_id(
            self,
            sid_snapshot_date: t.Optional[str | date] = None,
            custom_key: t.Optional[PredefinedKeys | str] = None,
        ) -> "Repseudonymize._RepseudoFuncSelectorTarget":
            """Claim that the selected fields were mapped to Stable ID, then pseudonymized with PAPIS-compatible encryption.

            Args:
                sid_snapshot_date (Optional[str | date], optional): Date representing SID-catalogue version that was used.
                    Latest if unspecified. Format: YYYY-MM-DD
                custom_key (Optional[PredefinedKeys | str], optional): Override the key to use for pseudonymization.
                    Must be one of the keys defined in PredefinedKeys. If not defined, uses the default key for this function (papis-common-key-1)

            Returns:
                An object with methods to choose how the field should be pseudonymized.
            """
            rules = super()._map_to_stable_id_and_pseudonymize(
                sid_snapshot_date, custom_key
            )
            return Repseudonymize._RepseudoFuncSelectorTarget(self.fields, rules)

        def from_default_encryption(
            self, custom_key: t.Optional[PredefinedKeys | str] = None
        ) -> "Repseudonymize._RepseudoFuncSelectorTarget":
            """Claim that the selected fields were pseudonymized with default encryption.

            Args:
                custom_key (Optional[PredefinedKeys | str], optional): Override the key to use for pseudonymization.
                    Must be one of the keys defined in PredefinedKeys. If not defined, uses the default key for this function (papis-common-key-1)

            Returns:
                An object with methods to choose how the field should be pseudonymized.
            """
            rules = super()._with_daead_encryption(custom_key)
            return Repseudonymize._RepseudoFuncSelectorTarget(self.fields, rules)

        def from_papis_compatible_encryption(
            self, custom_key: t.Optional[PredefinedKeys | str] = None
        ) -> "Repseudonymize._RepseudoFuncSelectorTarget":
            """Claim that the selected fields were pseudonymized with PAPIS-compatible encryption.

            Args:
                custom_key (Optional[PredefinedKeys | str], optional): Override the key to use for pseudonymization.
                    Must be one of the keys defined in PredefinedKeys. If not defined, uses the default key for this function (papis-common-key-1)

            Returns:
                An object with methods to choose how the field should be pseudonymized.
            """
            rules = super()._with_ff31_encryption(custom_key)
            return Repseudonymize._RepseudoFuncSelectorTarget(self.fields, rules)

        def from_custom_function(
            self, function: PseudoFunction
        ) -> "Repseudonymize._RepseudoFuncSelectorTarget":
            """Claim that the selected fields were pseudonymized with a custom, specified Pseudo Function."""
            rules = super()._with_custom_function(function)
            return Repseudonymize._RepseudoFuncSelectorTarget(self.fields, rules)

    class _RepseudoFuncSelectorTarget(_RuleConstructor):
        def __init__(self, fields: list[str], source_rules: list[PseudoRule]):
            self.source_rules = source_rules

            dataset_type: t.Literal["dataframe", "file"] = (
                "dataframe"
                if isinstance(Repseudonymize.dataset, pl.DataFrame)
                else "file"
            )
            super().__init__(fields, dataset_type)

        def to_stable_id(
            self,
            sid_snapshot_date: t.Optional[str | date] = None,
            custom_key: t.Optional[PredefinedKeys | str] = None,
        ) -> "Repseudonymize._Repseudonymizer":
            """Map the selected fields to Stable ID, then pseudonymize with a PAPIS-compatible encryption.

            In other words, this is a compound operation that both: 1) maps FNR to stable ID 2) then encrypts the Stable IDs.

            Args:
                sid_snapshot_date (Optional[str | date], optional): Date representing SID-catalogue version to use.
                    Latest if unspecified. Format: YYYY-MM-DD
                custom_key (Optional[PredefinedKeys | str], optional): Override the key to use for pseudonymization.
                    Must be one of the keys defined in PredefinedKeys. If not defined, uses the default key for this function (papis-common-key-1)

            Returns:
                Self: The object configured to be mapped to stable ID
            """
            rules = super()._map_to_stable_id_and_pseudonymize(
                sid_snapshot_date, custom_key
            )
            return Repseudonymize._Repseudonymizer(self.source_rules, rules)

        def to_default_encryption(
            self, custom_key: t.Optional[PredefinedKeys | str] = None
        ) -> "Repseudonymize._Repseudonymizer":
            """Pseudonymize the selected fields with the default encryption algorithm (DAEAD).

            Args:
                custom_key (Optional[PredefinedKeys | str], optional): Override the key to use for pseudonymization.
                    Must be one of the keys defined in PredefinedKeys. If not defined, uses the default key for this function (ssb-common-key-1)

            Returns:
                Self: The object configured to be mapped to stable ID
            """
            rules = super()._with_daead_encryption(custom_key)
            return Repseudonymize._Repseudonymizer(self.source_rules, rules)

        def to_papis_compatible_encryption(
            self, custom_key: t.Optional[PredefinedKeys | str] = None
        ) -> "Repseudonymize._Repseudonymizer":
            """Pseudonymize the selected fields with a PAPIS-compatible encryption algorithm (FF31).

            Args:
                custom_key (Optional[PredefinedKeys | str], optional): Override the key to use for pseudonymization.
                    Must be one of the keys defined in PredefinedKeys. If not defined, uses the default key for this function (papis-common-key-1)

            Returns:
                Self: The object configured to be mapped to stable ID
            """
            rules = super()._with_ff31_encryption(custom_key)
            return Repseudonymize._Repseudonymizer(self.source_rules, rules)

        def to_custom_function(
            self, function: PseudoFunction
        ) -> "Repseudonymize._Repseudonymizer":
            rules = super()._with_custom_function(function)
            return Repseudonymize._Repseudonymizer(self.source_rules, rules)
