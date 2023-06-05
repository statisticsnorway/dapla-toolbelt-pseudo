"""This module defines constants that are referenced throughout the codebase."""
from enum import Enum


class Env(str, Enum):
    """Environment variable keys."""

    PSEUDO_SERVICE_URL = "PSEUDO_SERVICE_URL"
    PSEUDO_SERVICE_AUTH_TOKEN = "PSEUDO_SERVICE_AUTH_TOKEN"  # noqa S105

    def __str__(self) -> str:
        """Use value for string representation."""
        return str(self.value)


class PseudoFunctionTypes(str, Enum):
    """Names of well known pseudo functions."""

    DAEAD = "daead"
    MAP_SID = "map-sid"
    FF31 = "ff31"

    def __str__(self) -> str:
        """Use value for string representation."""
        return str(self.value)


class PredefinedKeys(str, Enum):
    """Names of 'global keys' that the Dapla Pseudo Service is familiar with."""

    SSB_COMMON_KEY_1 = "ssb-common-key-1", PseudoFunctionTypes.DAEAD
    SSB_COMMON_KEY_2 = "ssb-common-key-2", PseudoFunctionTypes.DAEAD
    PAPIS_COMMON_KEY_1 = "papis-common-key-1", PseudoFunctionTypes.FF31

    def __new__(cls, value: str, pseudo_func_type: PseudoFunctionTypes):
        member = str.__new__(cls, value)
        member._value_ = value
        member.pseudo_func_type = pseudo_func_type
        return member

    def __str__(self) -> str:
        """Use value for string representation."""
        return str(self.value)


