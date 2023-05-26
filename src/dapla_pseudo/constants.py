"""This module defines constants that is referenced throughout the codebase."""
from enum import Enum


class Env(str, Enum):
    """Environment variables."""

    PSEUDO_SERVICE_URL = "PSEUDO_SERVICE_URL"
    PSEUDO_SERVICE_AUTH_TOKEN = "PSEUDO_SERVICE_AUTH_TOKEN"  # noqa S105


class PredefinedKeys(str, Enum):
    """Names of 'global keys' that the Dapla Pseudo Service is familiar with."""

    SSB_COMMON_KEY_1 = "ssb-common-key-1"
    SSB_COMMON_KEY_2 = "ssb-common-key-2"
    PAPIS_COMMON_KEY_1 = "papis-common-key-1"


class PseudoFunctionTypes(str, Enum):
    """Names of 'global keys' that the Dapla Pseudo Service is familiar with."""

    DAEAD = "daead"
    MAP_SID = "map-sid"
    FF31 = "ff31"
