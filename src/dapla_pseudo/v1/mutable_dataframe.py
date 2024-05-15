import re
import typing as t
from collections import Counter
from functools import cache
from io import BytesIO

import orjson
import polars as pl
from wcmatch import glob

from dapla_pseudo.v1.models.core import PseudoFunction
from dapla_pseudo.v1.models.core import PseudoRule

ARRAY_INDEX_MATCHER = re.compile(r"\[\d*]")


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
        func: PseudoFunction,  # "source func" if repseudo
        target_func: PseudoFunction | None,  # "target_func" if repseudo
    ) -> None:
        """Initialize the class."""
        self.path = path
        self.pattern = _ensure_normalized(pattern)
        self.col = col
        self.func = func
        self.target_func = target_func

    def update_col(self, key: str, data: list[str]) -> None:
        """Update the values in the matched column."""
        self.col.update({key: data})


class MutableDataFrame:
    """A dataframe that can change values in-place."""

    def __init__(self, dataframe: pl.DataFrame) -> None:
        """Initialize the class."""
        self.dataframe = dataframe
        self.dataframe_dict: t.Any = None
        self.matched_fields: list[FieldMatch] = []
        self.matched_fields_metrics: dict[str, int] | None = None

    def match_rules(
        self, rules: list[PseudoRule], target_rules: list[PseudoRule] | None
    ) -> None:
        """Create references to all the columns that matches the given pseudo rules."""
        counter: Counter[str] = Counter()
        self.dataframe_dict = orjson.loads(self.dataframe.write_json())
        self.matched_fields = list(
            _traverse_dataframe_dict(
                self.dataframe_dict["columns"],
                _combine_rules(rules, target_rules),
                counter,
            )
        )
        # The Counter contains unique field names. A count > 1 means that the traverse
        # was not able to group all values with a given path. This will be the case for
        # list of dicts.
        self.matched_fields_metrics = dict(counter)

    def get_matched_fields(self) -> list[FieldMatch]:
        """Get a reference to all the columns that matched pseudo rules."""
        return self.matched_fields

    def update(self, field: str, data: list[str]) -> None:
        """Update a column with the given data."""
        if any((field_match := f) for f in self.matched_fields if field == f.path):
            field_match.update_col("values", data)

    def to_polars(self) -> pl.DataFrame:
        """Convert to Polars DataFrame."""
        return (
            pl.read_json(BytesIO(orjson.dumps(self.dataframe_dict)))
            if self.dataframe_dict
            else self.dataframe
        )


def _combine_rules(
    rules: list[PseudoRule], target_rules: list[PseudoRule] | None
) -> list[tuple[PseudoRule, PseudoRule | None]]:
    combined: list[tuple[PseudoRule, PseudoRule | None]] = []

    # Zip rules and target_rules together; use None as target if target_rules is undefined
    for index, rule in enumerate(rules):
        combined.append((rule, target_rules[index] if target_rules else None))

    return combined


def _traverse_dataframe_dict(
    items: list[dict[str, t.Any] | None],
    rules: list[tuple[PseudoRule, PseudoRule | None]],
    metrics: Counter[str],
    prefix: str = "",
) -> t.Generator[FieldMatch, None, None]:
    for index, col in enumerate(items):
        if col is None:
            continue
        elif isinstance(col.get("datatype"), dict):
            next_prefix = (
                f"{prefix}[{index}]" if col["name"] == "" else f"{prefix}/{col['name']}"
            )
            yield from _traverse_dataframe_dict(
                col["values"], rules, metrics, next_prefix
            )
        elif len(col["values"]) > 0 and any(v is not None for v in col["values"]):
            name = f"{prefix}/{col['name']}".lstrip("/")
            for rule, target_rule in rules:
                if _glob_matches(_strip_array_indices(name), rule.pattern):
                    metrics.update({name: 1})
                    yield FieldMatch(
                        path=name,
                        col=col,
                        func=rule.func,
                        target_func=target_rule.func if target_rule else None,
                        pattern=rule.pattern,
                    )
                    break
    # print(_glob_matches.cache_info())


def _strip_array_indices(name: str) -> str:
    """Remove array index from the given path, e.g. remove the [1] part of /some/array[1]/struct/element.

    The array index is not used in glob matching anyway, and the result can be cached regardless of the array index
    """
    return ARRAY_INDEX_MATCHER.sub("", name)


@cache
def _glob_matches(name: str, rule: str) -> bool:
    """Use glob matching and cache the result."""
    return glob.globmatch(name.lower(), rule.lower(), flags=glob.GLOBSTAR | glob.BRACE)
