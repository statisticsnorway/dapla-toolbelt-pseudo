"""Builder for submitting a pseudonymization request."""

from datetime import date
from typing import ClassVar

import pandas as pd
import polars as pl

from dapla_pseudo.constants import TIMEOUT_DEFAULT
from dapla_pseudo.constants import MapFailureStrategy
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


class Depseudonymize:
    """Starting point for depseudonymization of datasets.

    This class should not be instantiated, only the static methods should be used.
    """

    dataset: File | pl.DataFrame

    @staticmethod
    def from_pandas(
        dataframe: pd.DataFrame, run_as_file: bool = False
    ) -> "Depseudonymize._Depseudonymizer":
        """Initialize a depseudonymization request from a pandas DataFrame."""
        dataset: pl.DataFrame = pl.from_pandas(dataframe)
        if run_as_file:
            file_handle, content_type = get_file_data_from_dataset(dataset)
            Depseudonymize.dataset = File(file_handle, content_type)
        else:
            Depseudonymize.dataset = dataset
        return Depseudonymize._Depseudonymizer()

    @staticmethod
    def from_polars(
        dataframe: pl.DataFrame, run_as_file: bool = False
    ) -> "Depseudonymize._Depseudonymizer":
        """Initialize a depseudonymization request from a polars DataFrame."""
        if run_as_file:
            file_handle, content_type = get_file_data_from_dataset(dataframe)
            Depseudonymize.dataset = File(file_handle, content_type)
        else:
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

    class _Depseudonymizer(_BasePseudonymizer):
        """Select one or multiple fields to be pseudonymized."""

        rules: ClassVar[list[PseudoRule]] = []

        def __init__(self, rules: list[PseudoRule] | None = None) -> None:
            """Initialize the class."""
            if rules is None:
                Depseudonymize._Depseudonymizer.rules = []
            else:
                Depseudonymize._Depseudonymizer.rules.extend(rules)

        def on_fields(self, *fields: str) -> "Depseudonymize._DepseudoFuncSelector":
            """Specify one or multiple fields to be depseudonymized."""
            return Depseudonymize._DepseudoFuncSelector(list(fields))

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
            )
            return super()._execute_pseudo_operation(self.rules, timeout, custom_keyset)

    class _DepseudoFuncSelector(_BaseRuleConstructor):
        def __init__(self, fields: list[str]) -> None:
            self._fields = fields
            super().__init__(fields, type(Depseudonymize.dataset))

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
            return Depseudonymize._Depseudonymizer(rules)

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
            return Depseudonymize._Depseudonymizer(rules)

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
            return Depseudonymize._Depseudonymizer(rules)

        def with_custom_function(
            self, function: PseudoFunction
        ) -> "Depseudonymize._Depseudonymizer":
            rules = super()._with_custom_function(function)
            return Depseudonymize._Depseudonymizer(rules)
