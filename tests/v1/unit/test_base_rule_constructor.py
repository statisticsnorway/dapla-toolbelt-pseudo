import polars as pl
import pytest_cases

from dapla_pseudo.constants import MapFailureStrategy
from dapla_pseudo.constants import PseudoFunctionTypes
from dapla_pseudo.utils import convert_to_date
from dapla_pseudo.v1.baseclasses import _BaseRuleConstructor
from dapla_pseudo.v1.models.core import DaeadKeywordArgs
from dapla_pseudo.v1.models.core import FF31KeywordArgs
from dapla_pseudo.v1.models.core import File
from dapla_pseudo.v1.models.core import MapSidKeywordArgs
from dapla_pseudo.v1.models.core import PseudoFunction
from dapla_pseudo.v1.models.core import PseudoRule


@pytest_cases.parametrize("dataset_type", [pl.DataFrame, File])
def test_map_to_stable_id_and_pseudonymize(
    dataset_type: type[pl.DataFrame] | type[File],
) -> None:
    """Purpose: Test that the rules are constructed as expected."""
    base = _BaseRuleConstructor(fields=["fnr", "fnr_2"], dataset_type=dataset_type)
    rules = base._map_to_stable_id_and_pseudonymize(
        sid_snapshot_date="2038-01-01",
        custom_key="ssb-common-key-2",
        on_map_failure="RETURN_NULL",
    )
    expected_func = PseudoFunction(
        function_type=PseudoFunctionTypes.MAP_SID,
        kwargs=MapSidKeywordArgs(
            key_id="ssb-common-key-2",
            snapshot_date=convert_to_date("2038-01-01"),
            failure_strategy=MapFailureStrategy("RETURN_NULL"),
        ),
    )
    if dataset_type == File:
        expected_rules = [
            PseudoRule(name=None, pattern="**/fnr", func=expected_func),
            PseudoRule(name=None, pattern="**/fnr_2", func=expected_func),
        ]
        assert rules == expected_rules
    else:
        expected_rules = [
            PseudoRule(name=None, pattern="fnr", func=expected_func),
            PseudoRule(name=None, pattern="fnr_2", func=expected_func),
        ]
        assert rules == expected_rules


@pytest_cases.parametrize("dataset_type", [pl.DataFrame, File])
def test_with_daead_encryption(
    dataset_type: type[pl.DataFrame] | type[File],
) -> None:
    """Purpose: Test that the rules are constructed as expected."""
    base = _BaseRuleConstructor(fields=["fnr", "fnr_2"], dataset_type=dataset_type)
    rules = base._with_daead_encryption(
        custom_key="ssb-common-key-2",
    )
    expected_func = PseudoFunction(
        function_type=PseudoFunctionTypes.DAEAD,
        kwargs=DaeadKeywordArgs(
            key_id="ssb-common-key-2",
        ),
    )
    if dataset_type == File:
        expected_rules = [
            PseudoRule(name=None, pattern="**/fnr", func=expected_func),
            PseudoRule(name=None, pattern="**/fnr_2", func=expected_func),
        ]
        assert rules == expected_rules
    else:
        expected_rules = [
            PseudoRule(name=None, pattern="fnr", func=expected_func),
            PseudoRule(name=None, pattern="fnr_2", func=expected_func),
        ]
        assert rules == expected_rules


@pytest_cases.parametrize("dataset_type", [pl.DataFrame, File])
def test_with_ff31_encryption(
    dataset_type: type[pl.DataFrame] | type[File],
) -> None:
    """Purpose: Test that the rules are constructed as expected."""
    base = _BaseRuleConstructor(fields=["fnr", "fnr_2"], dataset_type=dataset_type)
    rules = base._with_ff31_encryption(
        custom_key="ssb-common-key-2",
    )
    expected_func = PseudoFunction(
        function_type=PseudoFunctionTypes.FF31,
        kwargs=FF31KeywordArgs(
            key_id="ssb-common-key-2",
        ),
    )
    if dataset_type == File:
        expected_rules = [
            PseudoRule(name=None, pattern="**/fnr", func=expected_func),
            PseudoRule(name=None, pattern="**/fnr_2", func=expected_func),
        ]
        assert rules == expected_rules
    else:
        expected_rules = [
            PseudoRule(name=None, pattern="fnr", func=expected_func),
            PseudoRule(name=None, pattern="fnr_2", func=expected_func),
        ]
        assert rules == expected_rules
