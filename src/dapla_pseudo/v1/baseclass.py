import typing as t
from datetime import date

from dapla_pseudo.constants import MapFailureStrategy
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
        sid_snapshot_date: str | date | None = None,
        custom_key: PredefinedKeys | str | None = None,
        failure_strategy: MapFailureStrategy | None = None,
    ) -> list[PseudoRule]:
        kwargs = (
            MapSidKeywordArgs(
                key_id=custom_key,
                snapshot_date=convert_to_date(sid_snapshot_date),
                failure_strategy=failure_strategy,
            )
            if custom_key
            else MapSidKeywordArgs(
                snapshot_date=convert_to_date(sid_snapshot_date),
                failure_strategy=failure_strategy,
            )
        )
        pseudo_func = PseudoFunction(
            function_type=PseudoFunctionTypes.MAP_SID, kwargs=kwargs
        )
        return self._rule_constructor(pseudo_func)

    def _with_daead_encryption(
        self, custom_key: PredefinedKeys | str | None = None
    ) -> list[PseudoRule]:
        kwargs = (
            DaeadKeywordArgs(key_id=custom_key) if custom_key else DaeadKeywordArgs()
        )
        pseudo_func = PseudoFunction(
            function_type=PseudoFunctionTypes.DAEAD, kwargs=kwargs
        )
        return self._rule_constructor(pseudo_func)

    def _with_ff31_encryption(
        self, custom_key: PredefinedKeys | str | None = None
    ) -> list[PseudoRule]:
        kwargs = FF31KeywordArgs(key_id=custom_key) if custom_key else FF31KeywordArgs()
        pseudo_func = PseudoFunction(
            function_type=PseudoFunctionTypes.FF31, kwargs=kwargs
        )
        return self._rule_constructor(pseudo_func)

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
