"""This module defines helper classes and API models used to communicate with the Dapla Pseudo Service."""

import json
import typing as t
from datetime import date
from enum import Enum

from humps import camelize
from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import FieldSerializationInfo
from pydantic import ValidationError
from pydantic import field_serializer
from pydantic import model_serializer
from pydantic import model_validator

from dapla_pseudo.constants import MapFailureStrategy
from dapla_pseudo.constants import PredefinedKeys
from dapla_pseudo.constants import PseudoFunctionTypes
from dapla_pseudo.constants import UnknownCharacterStrategy
from dapla_pseudo.models import APIModel


class Mimetypes(str, Enum):
    """Mimetypes is an enum of supported mimetypes, for use in HTTP requests.

    As a proxy, this also defines the supported input file formats when reading from a file.
    """

    JSON = "application/json"
    CSV = "text/csv"


class Field(APIModel):
    """Field represents a targeted piece of data within a dataset or record.

    Parameters:
        pattern: field name or expression (e.g. a glob)
        mapping: If defined, denotes a mapping transformation that should be applied before the operation in question,
            e.g. "sid", meaning the field should be transformed to Stabil ID before being pseudonymized.
    """

    pattern: str
    mapping: str | None = None


class PseudoKeyset(APIModel):
    """PseudoKeyset represents a wrapped data encryption key (WDEK).

    Example structure, represented as JSON:
    {"encrypted_keyset": "CiQAp91NBhLdknX3j9jF6vwhdyURaqcT9/M/iczV7fLn...8XYFKwxiwMtCzDT6QGzCCCM=",
    "keyset_info": {
    "primaryKeyId": 1234567890,
    "keyInfo": [
    {
    "typeUrl": "type.googleapis.com/google.crypto.tink.AesSivKey",
    "status": "ENABLED",
    "keyId": 1234567890,
    "outputPrefixType": "TINK"
    }
    ]
    },
    "kek_uri": "gcp-kms://projects/some-project-id/locations/europe-north1/keyRings/some-keyring/cryptoKeys/some-kek-1"
    }
    """

    encrypted_keyset: str
    keyset_info: dict[str, t.Any]
    kek_uri: str

    def get_key_id(self) -> str:
        """ID of the keyset."""
        return str(self.keyset_info["primaryKeyId"])


class TargetCompression(APIModel):
    """TargetCompression denotes if and how results from the API should be compressed and password encrypted."""

    password: str


class KeyWrapper(BaseModel):
    """Hold information about a key, such as ID and keyset information."""

    key_id: str = ""
    keyset: PseudoKeyset | None = None

    def __init__(self, key: str | PseudoKeyset | None = None, **kwargs: t.Any) -> None:
        """Determine if a key is either a key reference (aka "common key") or a keyset.

        If it is a key reference, treat this as the key's ID, else retrieve the key's ID from the keyset data structure.

        :param key: either a key reference (as string) or a PseudoKeyset
        """
        super().__init__(**kwargs)
        if isinstance(key, str):
            try:  # Attempt to parse the key as a JSON-string matching the PseudoKeyset model
                pseudo_keyset = PseudoKeyset.model_validate(json.loads(key))
                self.key_id = pseudo_keyset.get_key_id()
                self.keyset = pseudo_keyset
                return
            except (ValidationError, json.JSONDecodeError):
                pass

            # Else, attempt to parse the key as one of the predefined keys
            if key in PredefinedKeys.__members__.values():
                self.key_id = key
                self.keyset = None
            else:
                raise ValueError(f"Key '{key}' is not a valid key reference or keyset")
        # Or we have an already parsed PseudoKeyset
        elif isinstance(key, PseudoKeyset):
            self.key_id = key.get_key_id()
            self.keyset = key

    def keyset_list(self) -> list[PseudoKeyset] | None:
        """Wrap the keyset in a list if it is defined - or return None if it is not."""
        return None if self.keyset is None else [self.keyset]


class PseudoFunctionArgs(BaseModel):
    """Representation of the possible keyword arguments."""

    def __str__(self) -> str:
        """As a default, represent the fields of the subclasses as kwargs on the format 'k=v'."""
        return ",".join(
            f"{k}={v}"
            for k, v in self.model_dump(by_alias=True).items()
            if v is not None
        )

    model_config = ConfigDict(alias_generator=camelize, populate_by_name=True)


class MapSidKeywordArgs(PseudoFunctionArgs):
    """Representation of kwargs for the 'map-sid' function.

    Parameters:
        key_id: The key to be used for pseudonomization.
        snapshot_date (date): The timestamp for the version of the SID catalogue.
            If not specified, will choose the latest version.
            The format is: YYYY-MM-DD, e.g. 2021-05-21
        strategy: defines how encryption/decryption should handle non-alphabet characters
        failure_strategy: defines how to handle mapping failures
    """

    key_id: PredefinedKeys | str = PredefinedKeys.PAPIS_COMMON_KEY_1
    snapshot_date: date | None = None
    strategy: UnknownCharacterStrategy | None = UnknownCharacterStrategy.SKIP
    failure_strategy: MapFailureStrategy | None = None


class DaeadKeywordArgs(PseudoFunctionArgs):
    """Representation of kwargs for the 'daead' function."""

    key_id: PredefinedKeys | str = PredefinedKeys.SSB_COMMON_KEY_1


class FF31KeywordArgs(PseudoFunctionArgs):
    """Representation of kwargs for the 'FF31' function."""

    key_id: PredefinedKeys | str = PredefinedKeys.PAPIS_COMMON_KEY_1
    strategy: UnknownCharacterStrategy | None = UnknownCharacterStrategy.SKIP


class RedactKeywordArgs(PseudoFunctionArgs):
    """Representation of kwargs for the 'redact' function."""

    placeholder: str | None = None
    regex: str | None = None


class PseudoFunction(BaseModel):
    """Formal representation of a pseudo function.

    Use to build up the string representation expected by pseudo service.

    Syntax: "<function_type>(<kwarg_1>=x, <kwarg_2>=y)"

    where <kwarg_1>, <kwarg_2>, etc. represents the keywords defined in PseudoFunctionArgs
    """

    function_type: PseudoFunctionTypes
    kwargs: DaeadKeywordArgs | FF31KeywordArgs | MapSidKeywordArgs | RedactKeywordArgs

    def __str__(self) -> str:
        """Create the function representation as expected by pseudo service."""
        return f"{self.function_type}({self.kwargs})"

    @model_serializer()
    def serialize_model(self) -> str:
        """Serialize the function as expected by the pseudo service."""
        return f"{self.function_type}({self.kwargs})"

    @model_validator(mode="before")
    @classmethod
    def deserialize_model(cls, data: str | dict) -> dict:
        """Deserialise the json-formatted pseudo function to Python model."""
        if isinstance(data, str):
            func: str
            args: str
            func, args = (
                data.replace(" ", "").replace("(", " ").replace(")", "").split(" ")
            )
            pseudo_function_type: PseudoFunctionTypes = PseudoFunctionTypes[
                func.upper()
            ]
            args_dict: dict[str, str] = dict(
                list(map(lambda v: v.split("="), args.split(",")))
            )
            return {
                "function_type": pseudo_function_type,
                "kwargs": cls._resolve_args(pseudo_function_type, args_dict),
            }
        else:
            return data

    @classmethod
    def _resolve_args(
        cls, pseudo_function_type: PseudoFunctionTypes, args: dict[str, str]
    ) -> DaeadKeywordArgs | FF31KeywordArgs | MapSidKeywordArgs | RedactKeywordArgs:
        match pseudo_function_type:
            case PseudoFunctionTypes.DAEAD:
                return DaeadKeywordArgs.model_validate(args)
            case PseudoFunctionTypes.REDACT:
                return RedactKeywordArgs.model_validate(args)
            case PseudoFunctionTypes.FF31:
                return FF31KeywordArgs.model_validate(args)
            case PseudoFunctionTypes.MAP_SID:
                return MapSidKeywordArgs.model_validate(args)


class PseudoRule(APIModel):
    """A ``PseudoRule`` defines a pattern, a transformation function, and optionally a friendly name of the rule.

    Each rule defines a glob pattern (see https://docs.oracle.com/javase/tutorial/essential/io/fileOps.html#glob)
    that identifies one or multiple fields, and a `func` that will be applied to the matching fields.

    Lists of PseudoRules are processed by the dapla-pseudo-service in the order they are defined, and only the first
    matching rule will be applied (thus: rule ordering is important).

    Parameters:
        name: A friendly name of the rule. This is optional, but can be handy for debugging
        pattern: Glob expression, such as: ``/**/{field1, field2, *navn}``
        func: A transformation function, such as ``tink-daead(<keyname>), redact(<replacementstring>) or fpe-anychar(<keyname>)``
    """

    name: str | None = None
    pattern: str
    func: PseudoFunction

    @field_serializer("func")
    def serialize_func(
        self, func: PseudoFunction, _info: FieldSerializationInfo
    ) -> str:
        """Explicit serialization of the 'func' field to coerce to string before serializing PseudoRule."""
        return str(func)

    @classmethod
    def from_json(cls, data: str) -> t.Any:
        """Deserialise the json-formatted pseudo rule to Python model."""
        return super().model_validate(eval(data))


class PseudoFieldRequest(APIModel):
    """Model of the pseudo field request sent to the service."""

    pseudo_func: PseudoFunction
    name: str
    values: list[str]
    keyset: PseudoKeyset | None = None


class DepseudoFieldRequest(APIModel):
    """Model of the depseudo field request sent to the service."""

    pseudo_func: PseudoFunction
    name: str
    values: list[str]
    keyset: PseudoKeyset | None = None


class RepseudoFieldRequest(APIModel):
    """Model of the repseudo field request sent to the service."""

    source_pseudo_func: PseudoFunction
    target_pseudo_func: PseudoFunction
    name: str
    values: list[str]
    source_keyset: PseudoKeyset | None = None
    target_keyset: PseudoKeyset | None = None


class PseudoConfig(APIModel):
    """PseudoConfig is a container for rules and keysets."""

    rules: list[PseudoRule]
    keysets: list[PseudoKeyset] | None = None


class PseudonymizeFileRequest(APIModel):
    """PseudonymizeFileRequest represents a request towards pseudonymize file API endpoints."""

    pseudo_config: PseudoConfig
    target_uri: str | None = None
    target_content_type: Mimetypes
    compression: TargetCompression | None = None


class DepseudonymizeFileRequest(APIModel):
    """DepseudonymizeFileRequest represents a request towards depseudonymize file API endpoints."""

    pseudo_config: PseudoConfig
    target_uri: str | None = None
    target_content_type: Mimetypes
    compression: TargetCompression | None = None


class RepseudonymizeFileRequest(APIModel):
    """RepseudonymizeFileRequest represents a request towards repseudonymize file API endpoints."""

    source_pseudo_config: PseudoConfig
    target_pseudo_config: PseudoConfig
    target_uri: str | None = None
    target_content_type: Mimetypes
    compression: TargetCompression | None = None
