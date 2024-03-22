import typing as t
from datetime import date

from dapla_pseudo.constants import PredefinedKeys
from dapla_pseudo.constants import PseudoFunctionTypes
from dapla_pseudo.utils import convert_to_date
from dapla_pseudo.v1.api_models import DaeadKeywordArgs
from dapla_pseudo.v1.api_models import FF31KeywordArgs
from dapla_pseudo.v1.api_models import MapSidKeywordArgs
from dapla_pseudo.v1.api_models import PseudoFunction
from dapla_pseudo.v1.api_models import PseudoRule


class _RuleConstructor:
    """RuleConstructor constructs PseudoRules based on the supplied fields and called methods."""

    def __init__(
        self, fields: list[str], dataset_type: t.Literal["file", "dataframe"]
    ) -> None:
        self._fields = fields
        self.dataset_type = dataset_type

    def _map_to_stable_id_and_pseudonymize(
        self,
        sid_snapshot_date: t.Optional[str | date] = None,
        custom_key: t.Optional[PredefinedKeys | str] = None,
    ) -> list[PseudoRule]:
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

    def _with_daead_encryption(
        self, custom_key: t.Optional[PredefinedKeys | str] = None
    ) -> list[PseudoRule]:
        """Pseudonymize the selected fields with the default encryption algorithm (DAEAD).

        Args:
            custom_key (Optional[PredefinedKeys | str], optional): Override the key to use for pseudonymization.
                Must be one of the keys defined in PredefinedKeys. If not defined, uses the default key for this function (ssb-common-key-1)

        Returns:
            Self: The object configured to be mapped to stable ID
        """
        kwargs = (
            DaeadKeywordArgs(key_id=custom_key) if custom_key else DaeadKeywordArgs()
        )
        function = PseudoFunction(
            function_type=PseudoFunctionTypes.DAEAD, kwargs=kwargs
        )
        return self._rule_constructor(function)

    def _with_ff31_encryption(
        self, custom_key: t.Optional[PredefinedKeys | str] = None
    ) -> list[PseudoRule]:
        """Pseudonymize the selected fields with a PAPIS-compatible encryption algorithm (FF31).

        Args:
            custom_key (Optional[PredefinedKeys | str], optional): Override the key to use for pseudonymization.
                Must be one of the keys defined in PredefinedKeys. If not defined, uses the default key for this function (papis-common-key-1)

        Returns:
            Self: The object configured to be mapped to stable ID
        """
        kwargs = FF31KeywordArgs(key_id=custom_key) if custom_key else FF31KeywordArgs()
        function = PseudoFunction(function_type=PseudoFunctionTypes.FF31, kwargs=kwargs)
        return self._rule_constructor(function)

    def _with_custom_function(self, function: PseudoFunction) -> list[PseudoRule]:
        return self._rule_constructor(function)

    def _rule_constructor(self, func: PseudoFunction) -> list[PseudoRule]:
        # If we use the pseudonymize_file endpoint, we need a glob catch-all prefix.
        rule_prefix = "**/" if self.dataset_type == "file" else ""
        rules = [
            PseudoRule(name=None, func=func, pattern=f"{rule_prefix}{field}")
            for field in self._fields
        ]
        return rules
