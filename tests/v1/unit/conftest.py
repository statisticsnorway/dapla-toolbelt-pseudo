import pytest_cases

from dapla_pseudo.constants import PseudoFunctionTypes
from dapla_pseudo.v1.models.core import DaeadKeywordArgs
from dapla_pseudo.v1.models.core import MapSidKeywordArgs
from dapla_pseudo.v1.models.core import PseudoFunction


@pytest_cases.fixture()
def pseudo_func_sid() -> PseudoFunction:
    """PseudoFunction fixture for DAEAD."""
    return PseudoFunction(
        function_type=PseudoFunctionTypes.MAP_SID, kwargs=MapSidKeywordArgs()
    )


@pytest_cases.fixture()
def pseudo_func_daead() -> PseudoFunction:
    """PseudoFunction fixture for DAEAD."""
    return PseudoFunction(
        function_type=PseudoFunctionTypes.DAEAD, kwargs=DaeadKeywordArgs()
    )
