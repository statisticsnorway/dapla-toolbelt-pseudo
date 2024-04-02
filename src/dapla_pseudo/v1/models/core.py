from dataclasses import dataclass
from typing import Any
from typing import Optional

import polars as pl

from dapla_pseudo.types import BinaryFileDecl
from dapla_pseudo.v1.models.api import Mimetypes


@dataclass
class File:
    """'File' represents a file to be pseudonymized."""

    file_handle: BinaryFileDecl
    content_type: Mimetypes


@dataclass
class RawPseudoMetadata:
    """RawPseudoMetadata holds the raw metadata obtained from Pseudo Service."""

    logs: list[str]
    metrics: list[str]
    datadoc: list[dict[str, Any]]
    field_name: Optional[str] = None


@dataclass
class PseudoFieldResponse:
    """PseudoFileResponse holds the data and metadata from a Pseudo Service field response."""

    data: pl.DataFrame
    raw_metadata: list[RawPseudoMetadata]


@dataclass
class PseudoFileResponse:
    """PseudoFileResponse holds the data and metadata from a Pseudo Service file response."""

    data: list[dict[str, Any]]
    raw_metadata: RawPseudoMetadata
    content_type: Mimetypes
    file_name: str
    streamed: bool = True
