"""This module defines constants that are referenced throughout the codebase."""

from enum import Enum

TIMEOUT_DEFAULT: int = 10 * 60  # seconds


class Env(str, Enum):
    """Environment variable keys."""

    PSEUDO_SERVICE_URL = "PSEUDO_SERVICE_URL"
    PSEUDO_SERVICE_AUTH_TOKEN = "PSEUDO_SERVICE_AUTH_TOKEN"  # S105

    def __str__(self) -> str:
        """Use value for string representation."""
        return str(self.value)


class PseudoOperation(str, Enum):
    """Pseudo operation."""

    PSEUDONYMIZE = "pseudonymize"
    DEPSEUDONYMIZE = "depseudonymize"
    REPSEUDONYMIZE = "repseudonymize"


class PredefinedKeys(str, Enum):
    """Names of 'global keys' that the Dapla Pseudo Service is familiar with."""

    SSB_COMMON_KEY_1 = "ssb-common-key-1"
    SSB_COMMON_KEY_2 = "ssb-common-key-2"
    PAPIS_COMMON_KEY_1 = "papis-common-key-1"

    def __str__(self) -> str:
        """Use value for string representation."""
        return str(self.value)


class PseudoFunctionTypes(str, Enum):
    """Names of well known pseudo functions."""

    DAEAD = "daead"
    MAP_SID = "map-sid-ff31"
    FF31 = "ff31"
    REDACT = "redact"

    def __str__(self) -> str:
        """Use value for string representation."""
        return str(self.value)


class UnknownCharacterStrategy(str, Enum):
    """UnknownCharacterStrategy defines how encryption/decryption should handle non-alphabet characters."""

    FAIL = "fail"
    SKIP = "skip"
    DELETE = "delete"
    REDACT = "redact"

    def __str__(self) -> str:
        """Use value for string representation."""
        return str(self.value)


class MapFailureStrategy(str, Enum):
    """UnknownCharacterStrategy defines how encryption/decryption should handle non-alphabet characters."""

    RETURN_NULL = "RETURN_NULL"
    RETURN_ORIGINAL = "RETURN_ORIGINAL"

    def __str__(self) -> str:
        """Use value for string representation."""
        return str(self.value)
