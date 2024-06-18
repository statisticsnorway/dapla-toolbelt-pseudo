"""Builder for submitting a pseudonymization request."""

import typing as t
from datetime import date

import pandas as pd
import polars as pl

from dapla_pseudo.constants import TIMEOUT_DEFAULT
from dapla_pseudo.constants import PredefinedKeys
from dapla_pseudo.constants import PseudoOperation
from dapla_pseudo.types import FileLikeDatasetDecl
from dapla_pseudo.utils import get_file_data_from_dataset
from dapla_pseudo.v1.baseclasses import _BasePseudonymizer
from dapla_pseudo.v1.baseclasses import _BaseRuleConstructor
from dapla_pseudo.v1.models.core import File
from dapla_pseudo.v1.models.core import PseudoFunction
from dapla_pseudo.v1.models.core import PseudoKeyset
from dapla_pseudo.v1.models.core import PseudoRule
from dapla_pseudo.v1.result import Result


class Repseudonymize:
    """Starting point for pseudonymization of datasets.

    This class should not be instantiated, only the static methods should be used.
    """

    dataset: File | pl.DataFrame

    @staticmethod
    def from_pandas(
        dataframe: pd.DataFrame, run_as_file: bool = False
    ) -> "Repseudonymize._Repseudonymizer":
        """Initialize a pseudonymization request from a pandas DataFrame."""
        dataset: pl.DataFrame = pl.from_pandas(dataframe)
        if run_as_file:
            file_handle, content_type = get_file_data_from_dataset(dataset)
            Repseudonymize.dataset = File(file_handle, content_type)
        else:
            Repseudonymize.dataset = dataset
        return Repseudonymize._Repseudonymizer()

    @staticmethod
    def from_polars(
        dataframe: pl.DataFrame, run_as_file: bool = False
    ) -> "Repseudonymize._Repseudonymizer":
        """Initialize a pseudonymization request from a polars DataFrame."""
        if run_as_file:
            file_handle, content_type = get_file_data_from_dataset(dataframe)
            Repseudonymize.dataset = File(file_handle, content_type)
        else:
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

    class _Repseudonymizer(_BasePseudonymizer):
        """Select one or multiple fields to be pseudonymized."""

        source_rules: t.ClassVar[list[PseudoRule]] = []
        target_rules: t.ClassVar[list[PseudoRule]] = []

        def __init__(
            self,
            source_rules: list[PseudoRule] | None = None,
            target_rules: list[PseudoRule] | None = None,
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

        def on_fields(
            self, *fields: str
        ) -> "Repseudonymize._RepseudoFuncSelectorSource":
            """Specify one or multiple fields to be pseudonymized."""
            return Repseudonymize._RepseudoFuncSelectorSource(list(fields))

        def run(
            self,
            hierarchical: bool = False,
            source_custom_keyset: PseudoKeyset | str | None = None,
            target_custom_keyset: PseudoKeyset | str | None = None,
            timeout: int = TIMEOUT_DEFAULT,
        ) -> Result:
            """Pseudonymize the dataset.

            Args:
                hierarchical (bool): Whether the dataset is hierarchical or not. Needs PseudoRules with concrete paths. Defaults to False.
                source_custom_keyset (PseudoKeyset, optional): The source pseudonymization keyset to use. Defaults to None.
                target_custom_keyset (PseudoKeyset, optional): The target pseudonymization keyset to use. Defaults to None.
                timeout (int): The timeout in seconds for the API call. Defaults to TIMEOUT_DEFAULT.

            Returns:
                Result: The pseudonymized dataset and the associated metadata.
            """
            super().__init__(
                pseudo_operation=PseudoOperation.REPSEUDONYMIZE,
                dataset=Repseudonymize.dataset,
                hierarchical=hierarchical,
            )

            return super()._execute_pseudo_operation(
                rules=self.source_rules,
                target_rules=self.target_rules,
                custom_keyset=source_custom_keyset,
                target_custom_keyset=target_custom_keyset,
                timeout=timeout,
            )

    class _RepseudoFuncSelectorSource(_BaseRuleConstructor):
        def __init__(self, fields: list[str]) -> None:
            self.fields = fields
            super().__init__(fields, type(Repseudonymize.dataset))

        def from_stable_id(
            self,
            sid_snapshot_date: str | date | None = None,
            custom_key: PredefinedKeys | str | None = None,
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
            self, custom_key: PredefinedKeys | str | None = None
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
            self, custom_key: PredefinedKeys | str | None = None
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

    class _RepseudoFuncSelectorTarget(_BaseRuleConstructor):
        def __init__(self, fields: list[str], source_rules: list[PseudoRule]) -> None:
            self.source_rules = source_rules
            super().__init__(fields, type(Repseudonymize.dataset))

        def to_stable_id(
            self,
            sid_snapshot_date: str | date | None = None,
            custom_key: PredefinedKeys | str | None = None,
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
            self, custom_key: PredefinedKeys | str | None = None
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
            self, custom_key: PredefinedKeys | str | None = None
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
