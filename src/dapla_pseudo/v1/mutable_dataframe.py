import typing as t
from io import BytesIO

import orjson
import polars as pl
from wcmatch import glob

from dapla_pseudo.v1.models.core import PseudoFunction
from dapla_pseudo.v1.models.core import PseudoRule


def _ensure_normalized(pattern: str) -> str:
    """Normalize the pattern.

    Ensure that the pattern always starts with a '/' or '*' to be compatible with the
    pattern matching that is used in pseudo-service.
    """
    return (
        pattern
        if (pattern.startswith("/") or pattern.startswith("*"))
        else "/" + pattern
    )


class FieldMatch:
    """Represents a reference to a matching column in the dataframe."""

    def __init__(
        self,
        path: str,
        pattern: str,
        col: dict[str, t.Any],
        func: PseudoFunction,
    ) -> None:
        """Initialize the class."""
        self.path = path
        self.pattern = _ensure_normalized(pattern)
        self.col = col
        self.func = func

    def update_col(self, key: str, data: list[str]) -> None:
        """Update the values in the matched column."""
        # self.col.update({key: data})
        self.col[key] = data


class MutableDataFrame:
    """A dataframe that can change values in-place."""

    def __init__(self, dataframe: pl.DataFrame) -> None:
        """Initialize the class."""
        self.dataframe_dict = orjson.loads(dataframe.write_json())
        self.matched_fields: list[FieldMatch] = []

    def match_rules(self, rules: list[PseudoRule]) -> None:
        """Create references to all the columns that matches the given pseudo rules."""
        self.matched_fields = _traverse_dataframe_dict(
            [], self.dataframe_dict["columns"], rules
        )

    def get_matched_fields(self) -> list[FieldMatch]:
        """Get a reference to all the columns that matched pseudo rules."""
        return self.matched_fields

    def update(self, field: str, data: list[str]) -> None:
        """Update a column with the given data."""
        if any((field_match := f) for f in self.matched_fields if field == f.path):
            field_match.update_col("values", data)

    def to_polars(self) -> pl.DataFrame:
        """Convert to Polars DataFrame."""
        return pl.read_json(BytesIO(orjson.dumps(self.dataframe_dict)))


def _traverse_dataframe_dict(
    accumulator: list[FieldMatch],
    items: list[dict[str, t.Any]],
    rules: list[PseudoRule],
    prefix: str = "",
) -> list[FieldMatch]:
    match: list[FieldMatch] = []
    for col in items:
        if col is None:
            pass
        elif isinstance(col["datatype"], dict):
            name = "[]" if col["name"] == "" else col["name"]
            match.extend(
                _traverse_dataframe_dict(
                    accumulator, col["values"], rules, f"{prefix}/{name}"
                )
            )
        else:
            name = f"{prefix}/{col['name']}".lstrip("/")
            if any((rule := r) for r in rules if _glob_matches(name, r.pattern)):
                match.append(
                    FieldMatch(path=name, col=col, func=rule.func, pattern=rule.pattern)
                )
    return match


def _glob_matches(name: str, rule: str) -> bool:
    return glob.globmatch(name, rule, flags=glob.GLOBSTAR)
