"""This module defines helper classes and API models used to communicate with the Dapla Pseudo Service."""

import typing as t
from dataclasses import dataclass

import polars as pl

from dapla_pseudo.models import APIModel
from dapla_pseudo.v1.models.core import Mimetypes
from dapla_pseudo.v1.models.core import PseudoConfig
from dapla_pseudo.v1.models.core import PseudoFunction
from dapla_pseudo.v1.models.core import PseudoKeyset
from dapla_pseudo.v1.models.core import PseudoRule
from dapla_pseudo.v1.models.core import TargetCompression


class FieldMatch(APIModel):
    """Model of the pseudo field request sent to the service."""

    path: str
    col: dict[str, t.Any]
    rule: PseudoRule

    def update_col(self, key: str, data: list[str]) -> None:
        self.col[key] = data


class PseudoFieldRequest(APIModel):
    """Model of the pseudo field request sent to the service."""

    pseudo_func: PseudoFunction
    name: str
    values: list[str]
    col: dict[str, t.Any]
    keyset: PseudoKeyset | None = None
    on_response: t.Callable[[str, list[str]], None] | None = None


class DepseudoFieldRequest(APIModel):
    """Model of the depseudo field request sent to the service."""

    pseudo_func: PseudoFunction
    name: str
    values: list[str]
    keyset: PseudoKeyset | None = None
    on_response: t.Callable[[str, list[str]], None] | None = None


class RepseudoFieldRequest(APIModel):
    """Model of the repseudo field request sent to the service."""

    source_pseudo_func: PseudoFunction
    target_pseudo_func: PseudoFunction
    name: str
    values: list[str]
    source_keyset: PseudoKeyset | None = None
    target_keyset: PseudoKeyset | None = None
    on_response: t.Callable[[str, list[str]], None] | None = None


class PseudoFileRequest(APIModel):
    """PseudonymizeFileRequest represents a request towards pseudonymize file API endpoints."""

    pseudo_config: PseudoConfig
    target_uri: str | None = None
    target_content_type: Mimetypes
    compression: TargetCompression | None = None


class DepseudoFileRequest(APIModel):
    """DepseudonymizeFileRequest represents a request towards depseudonymize file API endpoints."""

    pseudo_config: PseudoConfig
    target_uri: str | None = None
    target_content_type: Mimetypes
    compression: TargetCompression | None = None


class RepseudoFileRequest(APIModel):
    """RepseudonymizeFileRequest represents a request towards repseudonymize file API endpoints."""

    source_pseudo_config: PseudoConfig
    target_pseudo_config: PseudoConfig
    target_uri: str | None = None
    target_content_type: Mimetypes
    compression: TargetCompression | None = None


@dataclass
class RawPseudoMetadata:
    """RawPseudoMetadata holds the raw metadata obtained from Pseudo Service."""

    logs: list[str]
    metrics: list[str]
    datadoc: list[dict[str, t.Any]]
    field_name: str | None = None


@dataclass
class PseudoFieldResponse:
    """PseudoFileResponse holds the data and metadata from a Pseudo Service field response."""

    data: pl.DataFrame
    raw_metadata: list[RawPseudoMetadata]


@dataclass
class PseudoFileResponse:
    """PseudoFileResponse holds the data and metadata from a Pseudo Service file response."""

    data: list[dict[str, t.Any]]
    raw_metadata: RawPseudoMetadata
    content_type: Mimetypes
    file_name: str
    streamed: bool = True
