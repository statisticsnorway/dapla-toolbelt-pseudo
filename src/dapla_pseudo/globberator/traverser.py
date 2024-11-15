from collections.abc import Generator

import msgspec
import polars as pl
from dapla.gcs import GCSFileSystem
from wcmatch import glob

from dapla_pseudo.v1.models.core import PseudoRule


class SchemaTraverser:
    """Perform transformations and operations on a potentially nested Parquet schema."""

    def __init__(self, schema: dict[str, pl.DataType], rules: list[PseudoRule]) -> None:
        """Initialize the class."""
        self.schema = schema
        self.rules = rules

    @staticmethod
    def from_path(schema_path: str, rules_path: str) -> "SchemaTraverser":
        """Build a SchemaTraverser instance."""
        schema_opener = (
            GCSFileSystem().open if schema_path.startswith("gs://") else open
        )
        rules_opener = GCSFileSystem().open if rules_path.startswith("gs://") else open
        with schema_opener(schema_path, mode="rb") as schema_file:
            schema = pl.read_parquet_schema(schema_file)
        with rules_opener(schema_path, mode="rb") as rules_file:
            content = msgspec.json.decode(rules_file.read())
            rules = [msgspec.convert(rule, PseudoRule) for rule in content]

        return SchemaTraverser(schema, rules)

    @staticmethod
    def write_rules(file_path: str, rules: list[PseudoRule]) -> None:
        """Write rules to file."""
        opener = GCSFileSystem.open if file_path.startswith("gs://") else open
        with opener(file_path, mode="wb") as rules_file:
            rules_file.write(msgspec.json.encode(rules))

    def match_rules(self, separator: str = "/") -> list[PseudoRule]:
        """Match a set of glob patterns.

        Takes a set of PseudoRules *without* concrete paths, and
        maps the glob-patterns to a concrete /path/to/thing using the schema.
        """
        return list(
            SchemaTraverser._match_rules(self.schema, self.rules, "", separator)
        )

    @staticmethod
    def _match_rules(
        schema: dict[str, pl.DataType],
        rules: list[PseudoRule],
        prev_path: str,
        separator: str,
    ) -> Generator[PseudoRule, None, None]:
        for name, dtype in schema.items():
            # Example for Polars types:
            # dtype     = List[Struct[Int64]]
            # base_type = List
            base_type = dtype.base_type()
            path = f"{prev_path}{separator}{name}".lstrip("/")  # remove leading /
            match base_type:
                case pl.Struct:  # If struct, nest further
                    yield from SchemaTraverser._match_rules(
                        dtype.to_schema(), rules, path, separator  # type: ignore[attr-defined]
                    )

                case pl.List | pl.Array:  # If list-like type, nest with the inner value
                    inner_type = dtype.inner  # type: ignore[attr-defined]
                    if inner_type == pl.Struct:
                        yield from SchemaTraverser._match_rules(
                            inner_type.to_schema(), rules, path, separator
                        )
                    elif inner_type == pl.List or inner_type == pl.Array:
                        raise ValueError(
                            f"Nested type: {dtype} could not be parsed.\
                            Nested types within nested types are not supported"
                        )
                    else:  # If not nested type, match on rule
                        if (
                            rule := SchemaTraverser._match_rule(path, rules)
                        ) is not None:
                            yield PseudoRule(
                                path=path,
                                pattern=rule.pattern,
                                func=rule.func,
                                name=rule.name,
                            )
                case _:
                    if (rule := SchemaTraverser._match_rule(path, rules)) is not None:
                        yield PseudoRule(
                            path=path,
                            pattern=rule.pattern,
                            func=rule.func,
                            name=rule.name,
                        )

    @staticmethod
    def _match_rule(path: str, rules: list[PseudoRule]) -> PseudoRule | None:
        for rule in rules:
            path = path.lstrip("/")  # remove leading /
            if glob.globmatch(
                path.lower(), rule.pattern.lower(), flags=glob.GLOBSTAR | glob.BRACE
            ):
                return rule
        return None
