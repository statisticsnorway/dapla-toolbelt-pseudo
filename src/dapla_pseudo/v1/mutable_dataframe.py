import re
from collections import Counter
from collections.abc import Generator
from io import BytesIO
from typing import Any

import msgspec
import orjson
import polars as pl

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
        col: dict[str, Any] | list[str | int | None],
        func: PseudoFunction,  # "source func" if repseudo
        target_func: PseudoFunction | None,  # "target_func" if repseudo
    ) -> None:
        """Initialize the class."""
        self.path = path
        self.pattern = _ensure_normalized(pattern)
        self.col = col
        self.func = func
        self.target_func = target_func

    def get_value(self) -> list[str | int | None]:
        """Get the inner value.

        If hierarchical, get the values of the matched column.
        Otherwise, just return the data of the Polars DataFrame.
        """
        if isinstance(self.col, list):
            return self.col
        else:
            return self.col["values"]  # type: ignore[no-any-return]


class MutableDataFrame:
    """A DataFrame that can change values in-place.

    If the DataFrame is hierarchical
    """

    def __init__(self, dataframe: pl.DataFrame, hierarchical: bool) -> None:
        """Initialize the class."""
        self.dataset: pl.DataFrame | dict[str, Any] = dataframe
        self.matched_fields: dict[str, FieldMatch] = {}
        self.matched_fields_metrics: dict[str, int] | None = None
        self.hierarchical: bool = hierarchical
        self.schema = dataframe.schema

    def match_rules(
        self, rules: list[PseudoRule], target_rules: list[PseudoRule] | None
    ) -> None:
        """Create references to all the columns that matches the given pseudo rules."""
        if self.hierarchical is False:
            assert isinstance(self.dataset, pl.DataFrame)
            self.matched_fields = {
                str(i): FieldMatch(
                    path=rule.pattern,
                    pattern=rule.pattern,
                    col=list(self.dataset.get_column(rule.pattern)),
                    func=rule.func,
                    target_func=target_rule.func if target_rule else None,
                )
                for (i, (rule, target_rule)) in enumerate(
                    _combine_rules(rules, target_rules)
                )
            }
        else:
            counter: Counter[str] = Counter()
            assert isinstance(self.dataset, pl.DataFrame)
            self.dataset = msgspec.json.decode(self.dataset.serialize(format="json"))
            assert isinstance(self.dataset, dict)
            for source_rule, target_rule in _combine_rules(rules, target_rules):
                if source_rule.path is None:
                    raise ValueError(
                        f"Rule: {source_rule}\n does not have a concrete path, and cannot be used."
                    )
                matches = _traverse_dataframe_dict(
                    self.dataset["columns"],
                    (source_rule, target_rule),
                    source_rule.path.split("/"),
                    counter,
                )
                for match in matches:
                    self.matched_fields[match.path] = match

            # The Counter contains unique field names. A count > 1 means that the traverse
            # was not able to group all values with a given path. This will be the case for
            # list of dicts.
            self.matched_fields_metrics = dict(counter)

    def get_matched_fields(self) -> dict[str, FieldMatch]:
        """Get a reference to all the columns that matched pseudo rules."""
        return self.matched_fields

    def update(self, path: str, data: list[str]) -> None:
        """Update a column with the given data."""
        if self.hierarchical is False:
            assert isinstance(self.dataset, pl.DataFrame)
            self.dataset = self.dataset.with_columns(pl.Series(data).alias(path))
        elif (field_match := self.matched_fields.get(path)) is not None:
            assert isinstance(field_match.col, dict)
            field_match.col.update({"values": data})

    def to_polars(self) -> pl.DataFrame:
        """Convert to Polars DataFrame."""
        if self.hierarchical is False:
            assert isinstance(self.dataset, pl.DataFrame)
            return self.dataset
        else:
            return pl.DataFrame.deserialize(
                BytesIO(orjson.dumps(self.dataset)),
                format="json",
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
    items: list[dict[str, Any] | None],
    rules: tuple[PseudoRule, PseudoRule | None],
    curr_path: list[str],
    metrics: Counter[str],
    prefix: str = "",
) -> Generator[FieldMatch, None, None]:

    path_head, *path_tail = curr_path
    for index, col in enumerate(items):
        if col is None or curr_path == []:
            continue

        if col["name"] == "" and any(
            key in col["datatype"] for key in {"Struct", "List", "Array"}
        ):
            next_prefix = f"{prefix}[{index}]"
            yield from _traverse_dataframe_dict(
                col["values"],
                rules=rules,
                curr_path=curr_path,
                metrics=metrics,
                prefix=next_prefix,
            )
        elif col["name"] == path_head:
            next_prefix = f"{prefix}/{col['name']}"
            if path_tail == []:  # matched entire path
                rule, target_rule = rules

                if (
                    "List" in col["datatype"] or "Array" in col["datatype"]
                ):  # is pl.List or pl.Array
                    ## Special case: inner lists are weird and needs to be wrangled.
                    col = col["values"][0]
                    if col is None or len(col) == 0:
                        continue

                if not all(v is None for v in col["values"]):
                    metrics.update({next_prefix: 1})
                    yield FieldMatch(
                        path=next_prefix.lstrip("/"),
                        col=col,
                        func=rule.func,
                        target_func=target_rule.func if target_rule else None,
                        pattern=rule.pattern,
                    )
            else:
                yield from _traverse_dataframe_dict(
                    col["values"],
                    rules=rules,
                    curr_path=path_tail,
                    metrics=metrics,
                    prefix=next_prefix,
                )
