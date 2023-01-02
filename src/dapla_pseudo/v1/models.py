"""This module defines helper classes and API models used to communicate with the Dapla Pseudo Service."""
import json
import typing as t

from pydantic import BaseModel

from dapla_pseudo.models import APIModel


class PseudoRule(APIModel):
    """A ``PseudoRule`` defines a pattern, a transformation function, and optionally a friendly name of the rule.

    Each rule defines a glob pattern (see https://docs.oracle.com/javase/tutorial/essential/io/fileOps.html#glob)
    that identifies one or multiple fields, and a `func` that will be applied to the matching fields.

    Lists of PseudoRules are processed by the dapla-pseudo-service in the order they are defined, and only the first
    matching rule will be applied (thus: rule ordering is important).

    Attributes:
        name: A friendly name of the rule. This is optional, but can be handy for debugging
        pattern: Glob expression, such as: ``/**/{field1, field2, *navn}``
        func: A transformation function, such as ``tink-daead(<keyname>), redact(<replacementstring>) or fpe-anychar(<keyname>)``
    """

    name: t.Optional[str]
    pattern: str
    func: str


class Field(APIModel):
    """Field represents a targeted piece of data within a dataset or record.

    Attributes:
        pattern: field name or expression (e.g. a glob)
        mapping: If defined, denotes a mapping transformation that should be applied before the operation in question,
            e.g. "sid", meaning the field should be transformed to Stabil ID before being pseudonymized.
    """

    pattern: str
    mapping: t.Optional[str] = None


class PseudoKeyset(APIModel):
    """PseudoKeyset represents a wrapped data encryption key (WDEK)."""

    encrypted_keyset: str
    keyset_info: t.Dict[str, t.Any]
    kek_uri: str

    def get_key_id(self) -> str:
        """ID of the keyset."""
        return str(self.keyset_info["primaryKeyId"])


class PseudoConfig(APIModel):
    """PseudoConfig is a container for rules and keysets."""

    rules: t.List[PseudoRule]
    keysets: t.Optional[t.List[PseudoKeyset]]


class TargetCompression(APIModel):
    """TargetCompression denotes if and how results from the API should be compressed and password encrypted."""

    password: str


class PseudonymizeFileRequest(APIModel):
    """PseudonymizeFileRequest represents a request towards pseudonymize file API endpoints."""

    pseudo_config: PseudoConfig
    target_uri: t.Optional[str]
    target_content_type: str
    compression: t.Optional[TargetCompression]


class DepseudonymizeFileRequest(APIModel):
    """DepseudonymizeFileRequest represents a request towards depseudonymize file API endpoints."""

    pseudo_config: PseudoConfig
    target_uri: t.Optional[str]
    target_content_type: str
    compression: t.Optional[TargetCompression]


class RepseudonymizeFileRequest(APIModel):
    """RepseudonymizeFileRequest represents a request towards repseudonymize file API endpoints."""

    source_pseudo_config: PseudoConfig
    target_pseudo_config: PseudoConfig
    target_uri: t.Optional[str]
    target_content_type: str
    compression: t.Optional[TargetCompression]


class KeyWrapper(BaseModel):
    """Hold information about a key, such as ID and keyset information."""

    key_id: str = ""
    keyset: t.Union[PseudoKeyset, None] = None

    def __init__(self, key: t.Union[str, PseudoKeyset], **kwargs: t.Any):
        """Determine if a key is either a key reference (aka "common key") or a keyset.

        If it is a key reference, treat this as the key's ID, else retrieve the key's ID from the keyset data structure.

        :param key: either a key reference (as string) or a PseudoKeyset
        """
        super().__init__(**kwargs)
        if isinstance(key, str):
            # Either we have a keyset json
            if key.startswith("{"):
                pseudo_keyset = PseudoKeyset.parse_obj(json.loads(key))
                self.key_id = pseudo_keyset.get_key_id()
                self.keyset = pseudo_keyset
            # Either or a "key reference" (i.e. an id of a "common" key)
            else:
                self.key_id = key
                self.keyset = None
        # Or we have an already parsed PseudoKeyset
        elif isinstance(key, PseudoKeyset):
            self.key_id = key.get_key_id()
            self.keyset = key

    def keyset_list(self) -> t.Union[t.List[PseudoKeyset], None]:
        """Wrap the keyset in a list if it is defined - or return None if it is not."""
        return None if self.keyset is None else [self.keyset]
