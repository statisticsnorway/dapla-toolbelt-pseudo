"""Builder for submitting a pseudonymization request."""

import typing as t
from datetime import date

import pandas as pd
import polars as pl
from dapla_metadata.datasets.core import Datadoc

from dapla_pseudo.constants import TIMEOUT_DEFAULT
from dapla_pseudo.constants import PredefinedKeys
from dapla_pseudo.constants import PseudoOperation
from dapla_pseudo.v1.baseclasses import _BasePseudonymizer
from dapla_pseudo.v1.baseclasses import _BaseRuleConstructor
from dapla_pseudo.v1.models.core import PseudoFunction
from dapla_pseudo.v1.models.core import PseudoKeyset
from dapla_pseudo.v1.models.core import PseudoRule
from dapla_pseudo.v1.result import Result


class Repseudonymize:
    """Starting point for pseudonymization of datasets.

    This class should not be instantiated, only the static methods should be used.
    """

    dataset: pl.DataFrame | pl.LazyFrame
    schema: pd.Series | pl.Schema

    @staticmethod
    def from_pandas(dataframe: pd.DataFrame) -> "Repseudonymize._Repseudonymizer":
        """Initialize a pseudonymization request from a pandas DataFrame."""
        Repseudonymize.dataset = pl.from_pandas(dataframe)
        Repseudonymize.schema = dataframe.dtypes
        return Repseudonymize._Repseudonymizer()

    @staticmethod
    def from_polars(dataframe: pl.DataFrame) -> "Repseudonymize._Repseudonymizer":
        """Initialize a pseudonymization request from a polars DataFrame."""
        Repseudonymize.dataset = dataframe
        Repseudonymize.schema = dataframe.schema
        return Repseudonymize._Repseudonymizer()

    @staticmethod
    def from_polars_lazy(
        lazyframe: pl.LazyFrame,
    ) -> "Repseudonymize._Repseudonymizer":
        """Initialize a pseudonymization request from a polars LazyFrame.

        Args:
            lazyframe: A Polars LazyFrame.

        Returns:
            _Repseudonymizer: An instance of the _Repseudonymizer class.
        """
        Repseudonymize.dataset = lazyframe
        Repseudonymize.schema = Repseudonymize.dataset.collect_schema()
        return Repseudonymize._Repseudonymizer()

    class _Repseudonymizer(_BasePseudonymizer):
        """Select one or multiple fields to be pseudonymized."""

        source_rules: t.ClassVar[list[PseudoRule]] = []
        target_rules: t.ClassVar[list[PseudoRule]] = []
        metadata: Datadoc | None = None

        def __init__(
            self,
            source_rules: list[PseudoRule] | None = None,
            target_rules: list[PseudoRule] | None = None,
            metadata: Datadoc | None = None,
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

            self.metadata = metadata

        def on_fields(
            self, *fields: str
        ) -> "Repseudonymize._RepseudoFuncSelectorSource":
            """Specify one or multiple fields to be pseudonymized."""
            return Repseudonymize._RepseudoFuncSelectorSource(
                list(fields), self.metadata
            )

        def with_metadata(self, metadata: Datadoc) -> "Repseudonymize._Repseudonymizer":
            """Specify existing datadoc metadata for the dataset."""
            return Repseudonymize._Repseudonymizer(metadata=metadata)

        def run(
            self,
            hierarchical: bool = False,
            source_custom_keyset: PseudoKeyset | str | None = None,
            target_custom_keyset: PseudoKeyset | str | None = None,
            timeout: int = TIMEOUT_DEFAULT,
        ) -> Result:
            """Pseudonymize the dataset.

            Args:
                hierarchical: Whether the dataset is hierarchical or not. Needs PseudoRules with concrete paths. Defaults to False.
                source_custom_keyset: The source pseudonymization keyset to use. Defaults to None.
                target_custom_keyset: The target pseudonymization keyset to use. Defaults to None.
                timeout: The timeout in seconds for the API call. Defaults to TIMEOUT_DEFAULT.

            Returns:
                Result: The pseudonymized dataset and the associated metadata.

            Raises:
                ValueError: If hierarchical is True and input dataset is a Polars LazyFrame.
            """
            if hierarchical and isinstance(Repseudonymize.dataset, pl.LazyFrame):
                raise ValueError(
                    "Hierarchical datasets are not supported for Polars LazyFrames."
                )

            super().__init__(
                pseudo_operation=PseudoOperation.REPSEUDONYMIZE,
                dataset=Repseudonymize.dataset,
                hierarchical=hierarchical,
                user_provided_metadata=self.metadata,
            )

            result = super()._execute_pseudo_operation(
                rules=self.source_rules,
                target_rules=self.target_rules,
                custom_keyset=source_custom_keyset,
                target_custom_keyset=target_custom_keyset,
                timeout=timeout,
                schema=Repseudonymize.schema,
            )
            return result

    class _RepseudoFuncSelectorSource(_BaseRuleConstructor):
        def __init__(self, fields: list[str], metadata: Datadoc | None) -> None:
            self.fields = fields
            self._metadata = metadata
            super().__init__(fields)

        def from_stable_id(
            self,
            sid_snapshot_date: str | date | None = None,
            custom_key: PredefinedKeys | str | None = None,
        ) -> "Repseudonymize._RepseudoFuncSelectorTarget":
            """Claim that the selected fields were mapped to Stable ID, then pseudonymized with PAPIS-compatible encryption.

            Args:
                sid_snapshot_date: Date representing SID-catalogue version that was used.
                    Latest if unspecified. Format: YYYY-MM-DD
                custom_key: Override the key to use for pseudonymization.
                    Must be one of the keys defined in PredefinedKeys. If not defined, uses the default key for this function (papis-common-key-1)

            Returns:
                An object with methods to choose how the field should be pseudonymized.
            """
            rules = super()._map_to_stable_id_and_pseudonymize(
                sid_snapshot_date, custom_key
            )
            return Repseudonymize._RepseudoFuncSelectorTarget(
                self.fields, rules, self._metadata
            )

        def from_default_encryption(
            self, custom_key: PredefinedKeys | str | None = None
        ) -> "Repseudonymize._RepseudoFuncSelectorTarget":
            """Claim that the selected fields were pseudonymized with default encryption.

            Args:
                custom_key: Override the key to use for pseudonymization.
                    Must be one of the keys defined in PredefinedKeys. If not defined, uses the default key for this function (papis-common-key-1)

            Returns:
                An object with methods to choose how the field should be pseudonymized.
            """
            rules = super()._with_daead_encryption(custom_key)
            return Repseudonymize._RepseudoFuncSelectorTarget(
                self.fields, rules, self._metadata
            )

        def from_papis_compatible_encryption(
            self, custom_key: PredefinedKeys | str | None = None
        ) -> "Repseudonymize._RepseudoFuncSelectorTarget":
            """Claim that the selected fields were pseudonymized with PAPIS-compatible encryption.

            Args:
                custom_key: Override the key to use for pseudonymization.
                    Must be one of the keys defined in PredefinedKeys. If not defined, uses the default key for this function (papis-common-key-1)

            Returns:
                An object with methods to choose how the field should be pseudonymized.
            """
            rules = super()._with_ff31_encryption(custom_key)
            return Repseudonymize._RepseudoFuncSelectorTarget(
                self.fields, rules, self._metadata
            )

        def from_custom_function(
            self, function: PseudoFunction
        ) -> "Repseudonymize._RepseudoFuncSelectorTarget":
            """Claim that the selected fields were pseudonymized with a custom, specified Pseudo Function."""
            rules = super()._with_custom_function(function)
            return Repseudonymize._RepseudoFuncSelectorTarget(
                self.fields, rules, self._metadata
            )

    class _RepseudoFuncSelectorTarget(_BaseRuleConstructor):
        def __init__(
            self,
            fields: list[str],
            source_rules: list[PseudoRule],
            metadata: Datadoc | None,
        ) -> None:
            self.source_rules = source_rules
            self._metadata = metadata
            super().__init__(fields)

        def to_stable_id(
            self,
            sid_snapshot_date: str | date | None = None,
            custom_key: PredefinedKeys | str | None = None,
        ) -> "Repseudonymize._Repseudonymizer":
            """Map the selected fields to Stable ID, then pseudonymize with a PAPIS-compatible encryption.

            In other words, this is a compound operation that both: 1) maps FNR to stable ID 2) then encrypts the Stable IDs.

            Args:
                sid_snapshot_date: Date representing SID-catalogue version to use.
                    Latest if unspecified. Format: YYYY-MM-DD
                custom_key: Override the key to use for pseudonymization.
                    Must be one of the keys defined in PredefinedKeys. If not defined, uses the default key for this function (papis-common-key-1)

            Returns:
                Self: The object configured to be mapped to stable ID
            """
            rules = super()._map_to_stable_id_and_pseudonymize(
                sid_snapshot_date, custom_key
            )
            return Repseudonymize._Repseudonymizer(
                self.source_rules, rules, self._metadata
            )

        def to_default_encryption(
            self, custom_key: PredefinedKeys | str | None = None
        ) -> "Repseudonymize._Repseudonymizer":
            """Pseudonymize the selected fields with the default encryption algorithm (DAEAD).

            Args:
                custom_key: Override the key to use for pseudonymization.
                    Must be one of the keys defined in PredefinedKeys. If not defined, uses the default key for this function (ssb-common-key-1)

            Returns:
                Self: The object configured to be mapped to stable ID
            """
            rules = super()._with_daead_encryption(custom_key)
            return Repseudonymize._Repseudonymizer(
                self.source_rules, rules, self._metadata
            )

        def to_papis_compatible_encryption(
            self, custom_key: PredefinedKeys | str | None = None
        ) -> "Repseudonymize._Repseudonymizer":
            """Pseudonymize the selected fields with a PAPIS-compatible encryption algorithm (FF31).

            Args:
                custom_key: Override the key to use for pseudonymization.
                    Must be one of the keys defined in PredefinedKeys. If not defined, uses the default key for this function (papis-common-key-1)

            Returns:
                Self: The object configured to be mapped to stable ID
            """
            rules = super()._with_ff31_encryption(custom_key)
            return Repseudonymize._Repseudonymizer(
                self.source_rules, rules, self._metadata
            )

        def to_custom_function(
            self, function: PseudoFunction
        ) -> "Repseudonymize._Repseudonymizer":
            rules = super()._with_custom_function(function)
            return Repseudonymize._Repseudonymizer(
                self.source_rules, rules, self._metadata
            )
