"""This module defines helper classes and API models used to communicate with the Dapla Pseudo Service."""

import typing as t
from collections import defaultdict
from dataclasses import dataclass

import polars as pl
from pydantic import ConfigDict

from dapla_pseudo.models import APIModel
from dapla_pseudo.v1.models.core import PseudoFunction
from dapla_pseudo.v1.models.core import PseudoKeyset


class PseudoFieldRequest(APIModel):
    """Model of the pseudo field request sent to the service."""

    model_config = ConfigDict(hide_input_in_errors=True)

    pseudo_func: PseudoFunction
    name: str
    pattern: str
    values: list[str | int | None]
    keyset: PseudoKeyset | None = None


class DepseudoFieldRequest(APIModel):
    """Model of the depseudo field request sent to the service."""

    model_config = ConfigDict(hide_input_in_errors=True)

    pseudo_func: PseudoFunction
    name: str
    pattern: str
    values: list[str | int | None]
    keyset: PseudoKeyset | None = None


class RepseudoFieldRequest(APIModel):
    """Model of the repseudo field request sent to the service."""

    model_config = ConfigDict(hide_input_in_errors=True)

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
    metrics: list[dict[str, int]]
    datadoc: list[dict[str, t.Any]] | None
    field_name: str | None = None

    def __add__(self, other: "RawPseudoMetadata | None") -> "RawPseudoMetadata":
        """Combine two RawPseudoMetadata instances."""
        if other is None:
            return self
        grouped_metrics: dict[str, int] = defaultdict(int)
        for item in self.metrics + other.metrics:
            for key, value in item.items():
                grouped_metrics[key] += value
        grouped_metrics_unrolled = [{k: v} for k, v in grouped_metrics.items()]

        return RawPseudoMetadata(
            logs=self.logs + other.logs,
            metrics=grouped_metrics_unrolled,
            datadoc=self.datadoc or other.datadoc,
            field_name=self.field_name,
        )


@dataclass
class PseudoFieldResponse:
    """PseudoFieldResponse holds the data and metadata from a Pseudo Service field response."""

    data: pl.DataFrame | pl.LazyFrame
    raw_metadata: list[RawPseudoMetadata]
