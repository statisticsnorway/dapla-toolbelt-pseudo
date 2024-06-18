
from collections import OrderedDict

import polars as pl

from dapla_pseudo.globberator.traverser import SchemaTraverser
from dapla_pseudo.v1.models.core import PseudoRule


def test_schema_traverser() -> None:
    data = [
        {
            "identifiers": {"fnr": "11854898347", "dnr": "02099510504"},
            "names": [
                {"type": "nickname", "value": "matta"},
                {"type": "prefix", "value": "Sir"},
            ],
            "fornavn": "Mathias",
        },
        {
            "identifiers": {"fnr": "06097048531"}, "fornavn": "Gunnar"},
        {
            "identifiers": {"fnr": "02812289295"},
            "fnr": "02812289295",
            "fornavn": "Kristoffer",
        }
    ]
    rules = [
        PseudoRule.from_json(
            {"name": "my-rule", "pattern": "**/fnr", "func": "redact(placeholder=#)"}
        )
    ]
    
    df = pl.DataFrame(data)
    
    assert df.schema == OrderedDict(
        {
            "identifiers": pl.Struct({"fnr": pl.String, "dnr": pl.String}),
            "names": pl.List(pl.Struct({"type": pl.String, "value": pl.String})),
            "fornavn": pl.String,
            "fnr": pl.String,
        }
    )
    
    sc = SchemaTraverser(schema=df.schema, rules=rules)
    concrete_rules = sc.match_rules()
    
    assert concrete_rules == [
        PseudoRule.from_json(
            {"name": "my-rule", "pattern": "**/fnr", "path":"identifiers/fnr", "func": "redact(placeholder=#)"}
        ),
        PseudoRule.from_json(
            {"name": "my-rule", "pattern": "**/fnr", "path":"fnr", "func": "redact(placeholder=#)"}
        )
    ]