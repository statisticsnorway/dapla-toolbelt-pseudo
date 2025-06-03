"""Builder for submitting a pseudonymization request."""

from datetime import date
from typing import Any
from typing import ClassVar

import pandas as pd
import polars as pl
from datadoc_model.all_optional.model import MetadataContainer

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


class Pseudonymize:
    """Starting point for pseudonymization of datasets.

    This class should not be instantiated, only the static methods should be used.
    """

    dataset: File | pl.DataFrame
    prev_metadata: dict[str, dict[str, list[Any]]] | None  # Used in "from_result()"
    prev_datadoc: MetadataContainer | None  # Used in "from_result()"

    @staticmethod
    def from_pandas(
        dataframe: pd.DataFrame, run_as_file: bool = False
    ) -> "Pseudonymize._Pseudonymizer":
        """Initialize a pseudonymization request from a Pandas DataFrame.

        Args:
            dataframe: A Pandas DataFrame
            run_as_file: Force the dataset to be pseudonymized as a single file.

        Returns:
            _Pseudonymizer: An instance of the _Pseudonymizer class.
        """
        dataset: pl.DataFrame = pl.from_pandas(dataframe)
        Pseudonymize.prev_metadata = None
        Pseudonymize.prev_datadoc = None
        if run_as_file:
            file_handle, content_type = get_file_data_from_dataset(dataset)
            Pseudonymize.dataset = File(file_handle, content_type)
        else:
            Pseudonymize.dataset = pl.from_pandas(dataframe)
        return Pseudonymize._Pseudonymizer()

    @staticmethod
    def from_polars(
        dataframe: pl.DataFrame, run_as_file: bool = False
    ) -> "Pseudonymize._Pseudonymizer":
        """Initialize a pseudonymization request from a Polars DataFrame.

        Args:
            dataframe: A Polars DataFrame
            run_as_file: Force the dataset to be pseudonymized as a single file.

        Returns:
            _Pseudonymizer: An instance of the _Pseudonymizer class.
        """
        Pseudonymize.prev_metadata = None
        Pseudonymize.prev_datadoc = None
        if run_as_file:
            file_handle, content_type = get_file_data_from_dataset(dataframe)
            Pseudonymize.dataset = File(file_handle, content_type)
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
        Pseudonymize.prev_metadata = None
        Pseudonymize.prev_datadoc = None

        file_handle, content_type = get_file_data_from_dataset(dataset)
        Pseudonymize.dataset = File(file_handle, content_type)
        return Pseudonymize._Pseudonymizer()

    @staticmethod
    def from_result(
        result: Result, run_as_file: bool = False
    ) -> "Pseudonymize._Pseudonymizer":
        """Initialize a pseudonymization request from a previously computed Result.

        This allows the user to compose results from different pseudonymization operations,
        (pseudo/depseudo/repseudo), while preserving the metadata as it was a single run.
        This should not be used for operations of the same pseudo operation,
        in which case the builder pattern is preserved.

        Args:
            result: A previously pseudonymized DataFrame
            run_as_file: Force the dataset to be pseudonymized as a single file.

        Raises:
            ValueError: If the data structure in the "Result" object is not a DataFrame.

        Returns:
            _Pseudonymizer: An instance of the _Pseudonymizer class.

        Examples:
            result = (
                Pseudonymize
                    .from_polars(df)
                    .on_fields("fornavn","etternavn")
                    .with_default_encryption()
                    .run()
                )

            result = (
                Depseudonymize
                    .from_result(result)
                    .on_fields("bolig")
                    .with_default_encryption()
                    .run()
                )
            result.to_file("gs://ssb-play-obr-data-delt-ledstill-prod/")
        """
        Pseudonymize.prev_metadata = result._metadata
        Pseudonymize.prev_datadoc = result._datadoc

        if run_as_file:
            file_handle, content_type = get_file_data_from_dataset(result._pseudo_data)
            Pseudonymize.dataset = File(file_handle, content_type)
        else:
            if type(result._pseudo_data) is not pl.DataFrame:
                raise ValueError(
                    "Chaining pseudo results can only be done with DataFrames"
                )
            Pseudonymize.dataset = result._pseudo_data

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
            if (
                Pseudonymize.prev_datadoc is not None
                and Pseudonymize.prev_metadata is not None
            ):  # Add metadata from previous Result
                result.add_previous_metadata(
                    Pseudonymize.prev_metadata, Pseudonymize.prev_datadoc
                )
                return result
            else:
                return result

    class _PseudoFuncSelector(_BaseRuleConstructor):
        def __init__(self, fields: list[str]) -> None:
            self._fields = fields
            super().__init__(fields, type(Pseudonymize.dataset))

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
