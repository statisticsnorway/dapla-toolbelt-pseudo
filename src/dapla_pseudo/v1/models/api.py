"""This module defines helper classes and API models used to communicate with the Dapla Pseudo Service."""

import typing as t
from dataclasses import dataclass

import polars as pl

from dapla_pseudo.models import APIModel
from dapla_pseudo.v1.models.core import PseudoFunction
from dapla_pseudo.v1.models.core import PseudoKeyset


class PseudoFieldRequest(APIModel):
    """Model of the pseudo field request sent to the service."""

    pseudo_func: PseudoFunction
    name: str
    pattern: str
    values: list[str | int | None]  # 'int' is necessary to run redact on integers
    keyset: PseudoKeyset | None = None


class DepseudoFieldRequest(APIModel):
    """Model of the depseudo field request sent to the service."""

    pseudo_func: PseudoFunction
    name: str
    pattern: str
    values: list[str | int | None]
    keyset: PseudoKeyset | None = None


class RepseudoFieldRequest(APIModel):
    """Model of the repseudo field request sent to the service."""

    source_pseudo_func: PseudoFunction
    target_pseudo_func: PseudoFunction | None
    name: str
    pattern: str
    values: list[str | int | None]
    source_keyset: PseudoKeyset | None = None
    target_keyset: PseudoKeyset | None = None


@dataclass
class RawPseudoMetadata:
    """RawPseudoMetadata holds the raw metadata obtained from Pseudo Service."""

    logs: list[str]
    metrics: list[dict[str, t.Any]]
    datadoc: list[dict[str, t.Any]] | None
    field_name: str | None = None


@dataclass
class PseudoFieldResponse:
    """PseudoFieldResponse holds the data and metadata from a Pseudo Service field response."""

    data: pl.DataFrame
    raw_metadata: list[RawPseudoMetadata]
