"""Builder for submitting a pseudonymization request."""

from datetime import date
from typing import ClassVar

import pandas as pd
import polars as pl

from dapla_pseudo.constants import TIMEOUT_DEFAULT
from dapla_pseudo.constants import MapFailureStrategy
from dapla_pseudo.constants import PredefinedKeys
from dapla_pseudo.constants import PseudoOperation
from dapla_pseudo.v1.baseclasses import _BasePseudonymizer
from dapla_pseudo.v1.baseclasses import _BaseRuleConstructor
from dapla_pseudo.v1.models.core import PseudoFunction
from dapla_pseudo.v1.models.core import PseudoKeyset
from dapla_pseudo.v1.models.core import PseudoRule
from dapla_pseudo.v1.result import Result


class Pseudonymize:
    """Starting point for pseudonymization of datasets.

    This class should not be instantiated, only the static methods should be used.
    """

    dataset: pl.DataFrame

    @staticmethod
    def from_pandas(dataframe: pd.DataFrame) -> "Pseudonymize._Pseudonymizer":
        """Initialize a pseudonymization request from a Pandas DataFrame.

        Args:
            dataframe: A Pandas DataFrame

        Returns:
            _Pseudonymizer: An instance of the _Pseudonymizer class.
        """
        Pseudonymize.dataset = pl.from_pandas(dataframe)
        return Pseudonymize._Pseudonymizer()

    @staticmethod
    def from_polars(dataframe: pl.DataFrame) -> "Pseudonymize._Pseudonymizer":
        """Initialize a pseudonymization request from a Polars DataFrame.

        Args:
            dataframe: A Polars DataFrame

        Returns:
            _Pseudonymizer: An instance of the _Pseudonymizer class.
        """
        Pseudonymize.dataset = dataframe
        return Pseudonymize._Pseudonymizer()

    class _Pseudonymizer(_BasePseudonymizer):
        """Select one or multiple fields to be pseudonymized."""

        rules: ClassVar[list[PseudoRule]] = []

        def __init__(self, rules: list[PseudoRule] | None = None) -> None:
            """Initialize the class."""
            if rules is None:
                Pseudonymize._Pseudonymizer.rules = []
            else:
                Pseudonymize._Pseudonymizer.rules.extend(rules)

        def on_fields(self, *fields: str) -> "Pseudonymize._PseudoFuncSelector":
            """Specify one or multiple fields to be pseudonymized."""
            return Pseudonymize._PseudoFuncSelector(list(fields))

        def add_rules(
            self, rules: PseudoRule | list[PseudoRule]
        ) -> "Pseudonymize._Pseudonymizer":
            """Add one or more rules to existing pseudonymization rules."""
            if isinstance(rules, list):
                return Pseudonymize._Pseudonymizer(self.rules + rules)
            else:
                return Pseudonymize._Pseudonymizer([*self.rules, rules])

        def run(
            self,
            hierarchical: bool = False,
            custom_keyset: PseudoKeyset | str | None = None,
            timeout: int = TIMEOUT_DEFAULT,
        ) -> Result:
            """Pseudonymize the dataset.

            Args:
                hierarchical (bool): Whether the dataset is hierarchical or not. Needs PseudoRules with concrete paths. Defaults to False.
                custom_keyset (PseudoKeyset, optional): The pseudonymization keyset to use. Defaults to None.
                timeout (int): The timeout in seconds for the API call. Defaults to TIMEOUT_DEFAULT.

            Returns:
                Result: The pseudonymized dataset and the associated metadata.
            """
            super().__init__(
                pseudo_operation=PseudoOperation.PSEUDONYMIZE,
                dataset=Pseudonymize.dataset,
                hierarchical=hierarchical,
            )

            result = super()._execute_pseudo_operation(
                self.rules, timeout, custom_keyset
            )
            return result

    class _PseudoFuncSelector(_BaseRuleConstructor):
        def __init__(self, fields: list[str]) -> None:
            self._fields = fields
            super().__init__(fields)

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
            rules = super()._map_to_stable_id_and_pseudonymize(
                sid_snapshot_date, custom_key, on_map_failure
            )
            return Pseudonymize._Pseudonymizer(rules)

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
            rules = super()._with_daead_encryption(custom_key)
            return Pseudonymize._Pseudonymizer(rules)

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
            rules = super()._with_ff31_encryption(custom_key)
            return Pseudonymize._Pseudonymizer(rules)

        def with_custom_function(
            self, function: PseudoFunction
        ) -> "Pseudonymize._Pseudonymizer":
            rules = super()._with_custom_function(function)
            return Pseudonymize._Pseudonymizer(rules)
