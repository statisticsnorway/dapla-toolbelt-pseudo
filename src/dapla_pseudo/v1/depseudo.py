"""Builder for submitting a pseudonymization request."""

from datetime import date
from typing import ClassVar

import pandas as pd
import polars as pl
from dapla_metadata.datasets.core import Datadoc

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


class Depseudonymize:
    """Starting point for depseudonymization of datasets.

    This class should not be instantiated, only the static methods should be used.
    """

    dataset: pl.DataFrame
    schema: pd.Series | pl.Schema

    @staticmethod
    def from_pandas(dataframe: pd.DataFrame) -> "Depseudonymize._Depseudonymizer":
        """Initialize a depseudonymization request from a pandas DataFrame."""
        Depseudonymize.dataset = pl.from_pandas(dataframe)
        Depseudonymize.schema = dataframe.dtypes
        return Depseudonymize._Depseudonymizer()

    @staticmethod
    def from_polars(dataframe: pl.DataFrame) -> "Depseudonymize._Depseudonymizer":
        """Initialize a depseudonymization request from a polars DataFrame."""
        Depseudonymize.dataset = dataframe
        Depseudonymize.schema = dataframe.schema
        return Depseudonymize._Depseudonymizer()

    class _Depseudonymizer(_BasePseudonymizer):
        """Select one or multiple fields to be pseudonymized."""

        rules: ClassVar[list[PseudoRule]] = []
        metadata: Datadoc | None = None

        def __init__(
            self,
            rules: list[PseudoRule] | None = None,
            metadata: Datadoc | None = None,
        ) -> None:
            """Initialize the class."""
            if rules is None:
                Depseudonymize._Depseudonymizer.rules = []
            else:
                Depseudonymize._Depseudonymizer.rules.extend(rules)

            self.metadata = metadata

        def with_metadata(self, metadata: Datadoc) -> "Depseudonymize._Depseudonymizer":
            """Specify existing datadoc metadata for the dataset."""
            return Depseudonymize._Depseudonymizer(self.rules, metadata)

        def on_fields(self, *fields: str) -> "Depseudonymize._DepseudoFuncSelector":
            """Specify one or multiple fields to be depseudonymized."""
            return Depseudonymize._DepseudoFuncSelector(list(fields), self.metadata)

        def run(
            self,
            hierarchical: bool = False,
            custom_keyset: PseudoKeyset | str | None = None,
            timeout: int = TIMEOUT_DEFAULT,
        ) -> Result:
            """Depseudonymize the dataset.

            Args:
                hierarchical (bool): Whether the dataset is hierarchical or not. Needs PseudoRules with concrete paths. Defaults to False.
                custom_keyset (PseudoKeyset | str, optional): The depseudonymization keyset to use.
                    This can either be a PseudoKeyset, a JSON-string matching the fields of PseudoKeyset,
                    or a string matching one of the keys in `dapla_pseudo.constants.PredefinedKeys`. the Defaults to None.
                timeout (int): The timeout in seconds for the API call. Defaults to TIMEOUT_DEFAULT.

            Returns:
                Result: The depseudonymized dataset and the associated metadata.
            """
            super().__init__(
                pseudo_operation=PseudoOperation.DEPSEUDONYMIZE,
                dataset=Depseudonymize.dataset,
                hierarchical=hierarchical,
                user_provided_metadata=self.metadata,
            )

            result = super()._execute_pseudo_operation(
                self.rules, timeout, custom_keyset, schema=Depseudonymize.schema
            )
            return result

    class _DepseudoFuncSelector(_BaseRuleConstructor):
        def __init__(self, fields: list[str], metadata: Datadoc | None) -> None:
            self._fields = fields
            self._metadata = metadata
            super().__init__(fields)

        def with_stable_id(
            self,
            sid_snapshot_date: str | date | None = None,
            custom_key: str | None = None,
            on_map_failure: MapFailureStrategy | str | None = None,
        ) -> "Depseudonymize._Depseudonymizer":
            """Depseudonymize the selected fields with the default encryption algorithm (DAEAD).

            1) Decrypt stable-id
            2) Then map decrypted stable-id to fnr and return original fnr.

            Args:
                sid_snapshot_date (Optional[str | date], optional): Date representing SID-catalogue version to use.
                    Latest if unspecified. Format: YYYY-MM-DD
                custom_key (Optional[PredefinedKeys | str], optional): Override the key to use for pseudonymization.
                    Must be one of the keys defined in PredefinedKeys. If not defined, uses the default key for this function (papis-common-key-1)
                on_map_failure (Optional[MapFailureStrategy], optional): defines how to handle mapping failures

            Returns:
                Self: The object configured to be mapped to fnr
            """
            rules = super()._map_to_stable_id_and_pseudonymize(
                sid_snapshot_date, custom_key, on_map_failure
            )
            return Depseudonymize._Depseudonymizer(rules, self._metadata)

        def with_default_encryption(
            self, custom_key: PredefinedKeys | str | None = None
        ) -> "Depseudonymize._Depseudonymizer":
            """Depseudonymize the selected fields with the default encryption algorithm (DAEAD).

            Args:
                custom_key (Optional[PredefinedKeys | str], optional): Override the key to use for pseudonymization.
                    Must be one of the keys defined in PredefinedKeys. If not defined, uses the default key for this function (ssb-common-key-1)

            Returns:
                Self: The object configured to be mapped to stable ID
            """
            rules = super()._with_daead_encryption(custom_key)
            return Depseudonymize._Depseudonymizer(rules, self._metadata)

        def with_papis_compatible_encryption(
            self, custom_key: PredefinedKeys | str | None = None
        ) -> "Depseudonymize._Depseudonymizer":
            """Depseudonymize the selected fields with a PAPIS-compatible encryption algorithm (FF31).

            Args:
                custom_key (Optional[PredefinedKeys | str], optional): Override the key to use for pseudonymization.
                    Must be one of the keys defined in PredefinedKeys. If not defined, uses the default key for this function (papis-common-key-1)

            Returns:
                Self: The object configured to be mapped to stable ID
            """
            rules = super()._with_ff31_encryption(custom_key)
            return Depseudonymize._Depseudonymizer(rules, self._metadata)

        def with_custom_function(
            self, function: PseudoFunction
        ) -> "Depseudonymize._Depseudonymizer":
            rules = super()._with_custom_function(function)
            return Depseudonymize._Depseudonymizer(rules, self._metadata)
