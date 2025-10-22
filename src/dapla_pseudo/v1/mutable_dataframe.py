import re
from collections import Counter
from collections.abc import Generator
from typing import Any

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
        indexer: list[str | int],
        col: list[Any],
        wrapped_list: bool,
        func: PseudoFunction,  # "source func" if repseudo
        target_func: PseudoFunction | None,  # "target_func" if repseudo
    ) -> None:
        """Initialize the class."""
        self.path = path
        self.pattern = _ensure_normalized(pattern)
        self.indexer = indexer
        self.col = col
        self.wrapped_list = wrapped_list
        self.func = func
        self.target_func = target_func

    def get_value(self) -> list[str | int | None]:
        """Get the inner value.

        If hierarchical, get the values of the matched column.
        Otherwise, just return the data of the Polars DataFrame.
        """
        return self.col


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
                    indexer=[],
                    col=list(self.dataset.get_column(rule.pattern)),
                    wrapped_list=False,
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
            self.dataset = self.dataset.to_dict(as_series=False)
            assert isinstance(self.dataset, dict)
            for source_rule, target_rule in _combine_rules(rules, target_rules):
                if source_rule.path is None:
                    raise ValueError(
                        f"Rule: {source_rule}\n does not have a concrete path, and cannot be used."
                    )
                matches = _search_nested_path(
                    self.dataset,
                    source_rule.path,
                    (source_rule, target_rule),
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

    def update(self, path: str, data: list[str | None]) -> None:
        """Update a column with the given data."""
        if self.hierarchical is False:
            assert isinstance(self.dataset, pl.DataFrame)
            self.dataset = self.dataset.with_columns(pl.Series(data).alias(path))
        elif (field_match := self.matched_fields.get(path)) is not None:
            assert isinstance(self.dataset, dict)
            tmp = self.dataset
            for idx in field_match.indexer[:-1]:
                tmp = tmp[idx]  # type: ignore[index]
            tmp[field_match.indexer[-1]] = data if field_match.wrapped_list is False else data[0]  # type: ignore[index]

    def to_polars(self) -> pl.DataFrame:
        """Convert to Polars DataFrame."""
        if self.hierarchical is False:
            assert isinstance(self.dataset, pl.DataFrame)
            return self.dataset
        else:
            assert isinstance(self.dataset, dict)
            return pl.from_dict(self.dataset, schema_overrides=self.schema)


def _combine_rules(
    rules: list[PseudoRule], target_rules: list[PseudoRule] | None
) -> list[tuple[PseudoRule, PseudoRule | None]]:
    combined: list[tuple[PseudoRule, PseudoRule | None]] = []

    # Zip rules and target_rules together; use None as target if target_rules is undefined
    for index, rule in enumerate(rules):
        combined.append((rule, target_rules[index] if target_rules else None))

    return combined


def _search_nested_path(
    data: dict[str, Any] | list[Any],
    path: str,
    rules: tuple[PseudoRule, PseudoRule | None],
) -> Generator[FieldMatch, None, None]:
    """Search in the hierarchical data structure for the data at a given path.

    Args:
        data (dict[str, Any] | list[Any]): The hierarchical data structure to search.
        path (str): The path to search for in the data structure.
        rules (tuple[PseudoRule, PseudoRule  |  None]): The pseudo rules for the path.

    Yields:
        Generator[FieldMatch, None, None]: A generator yielding FieldMatch objects.
    """
    keys = path.strip("/").split("/")

    def _search(
        current: dict[str, Any] | list[Any],
        remaining_keys: list[str],
        rules: tuple[PseudoRule, PseudoRule | None],
        indexer: list[str | int],
        curr_path: list[str],
    ) -> Generator[FieldMatch, None, None]:
        if not remaining_keys:
            rule, target_rules = rules
            wrap_in_list = isinstance(current, list) is False
            yield FieldMatch(
                path="/".join(curr_path),
                pattern=rule.pattern,
                indexer=indexer,
                col=([current] if wrap_in_list else current),
                wrapped_list=wrap_in_list,
                func=rule.func,
                target_func=target_rules.func if target_rules else None,
            )
            return

        key = remaining_keys[0]
        if isinstance(current, dict):
            if key in current:
                yield from _search(
                    current[key],
                    remaining_keys[1:],
                    rules,
                    [*indexer, key],
                    [*curr_path, key],
                )

        elif isinstance(current, list):
            for idx, item in enumerate(current):
                if isinstance(item, dict):
                    if len(remaining_keys) == 1 and key in item:
                        rule, target_rules = rules

                        wrap_in_list = isinstance(item[key], list) is False
                        yield FieldMatch(
                            path=f"{'/'.join(curr_path)}[{idx}]/{key}",
                            pattern=rule.pattern,
                            indexer=[*indexer, idx, key],
                            col=([item[key]] if wrap_in_list else item[key]),
                            wrapped_list=wrap_in_list,
                            func=rule.func,
                            target_func=target_rules.func if target_rules else None,
                        )
                    else:
                        yield from _search(
                            item,
                            remaining_keys,
                            rules,
                            [*indexer, idx],
                            [*curr_path[:-1], f"{curr_path[-1]}[{idx}]"],
                        )
                elif isinstance(item, list):
                    yield from _search(
                        item,
                        remaining_keys,
                        rules,
                        [*indexer, idx],
                        [*curr_path[:-1], f"{curr_path[-1]}[{idx}]"],
                    )

    yield from _search(data, keys, rules, [], [])
