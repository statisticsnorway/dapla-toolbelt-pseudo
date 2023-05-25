"""This module defines constants that is referenced throughout the codebase."""
from typing import Final

from pydantic import BaseModel


class Env(BaseModel):
    """Environment variables."""

    PSEUDO_SERVICE_URL: Final[str] = "PSEUDO_SERVICE_URL"
    PSEUDO_SERVICE_AUTH_TOKEN: Final[str] = "PSEUDO_SERVICE_AUTH_TOKEN"


class PredefinedKeys(BaseModel):
    """Names of 'global keys' that the Dapla Pseudo Service is familiar with."""

    SSB_COMMON_KEY_1: Final[str] = "ssb-common-key-1"
    SSB_COMMON_KEY_2: Final[str] = "ssb-common-key-2"
    PAPIS_COMMON_KEY_1: Final[str] = "papis-common-key-1"


class PseudoFunctionTypes(BaseModel):
    """Names of 'global keys' that the Dapla Pseudo Service is familiar with."""

    DAEAD: Final[str] = "daead"
    MAP_SID: Final[str] = "map-sid"


env = Env()
predefined_keys = PredefinedKeys()
pseudo_function_types = PseudoFunctionTypes()
