import json

from dapla_pseudo.v1.models import Field
from dapla_pseudo.v1.models import PseudoConfig
from dapla_pseudo.v1.ops import _rules_of


def test_generate_rules_from_single_field() -> None:
    rules = _rules_of(key="some-key", fields=["some-field"], sid=[])
    assert PseudoConfig(rules=rules).to_json() == json.dumps(
        {"rules": [{"name": "rule-1", "pattern": "**/some-field", "func": "tink-daead(some-key)"}]}
    )


def test_generate_rules_from_multiple_field() -> None:
    rules = _rules_of(key="some-key", fields=["some-field", "another-field", "yet-another-field"], sid=[])
    assert PseudoConfig(rules=rules).to_json() == json.dumps(
        {
            "rules": [
                {"name": "rule-1", "pattern": "**/some-field", "func": "tink-daead(some-key)"},
                {"name": "rule-2", "pattern": "**/another-field", "func": "tink-daead(some-key)"},
                {"name": "rule-3", "pattern": "**/yet-another-field", "func": "tink-daead(some-key)"},
            ]
        }
    )


def test_generate_rules_from_hierarchy_field() -> None:
    rules = _rules_of(key="some-key", fields=["path/to/*-field"], sid=[])
    assert PseudoConfig(rules=rules).to_json() == json.dumps(
        {"rules": [{"name": "rule-1", "pattern": "**/path/to/*-field", "func": "tink-daead(some-key)"}]}
    )


def test_generate_rules_from_different_field_representations() -> None:
    rules = _rules_of(
        key="some-key",
        fields=[
            "string-field",
            {"pattern": "dict-field"},
            {"pattern": "dict-field-sid", "mapping": "sid"},
            Field(pattern="class-field"),
            Field(pattern="class-field-sid", mapping="sid"),
        ],
        sid=[],
    )
    assert PseudoConfig(rules=rules).to_json() == json.dumps(
        {
            "rules": [
                {"name": "rule-1", "pattern": "**/string-field", "func": "tink-daead(some-key)"},
                {"name": "rule-2", "pattern": "dict-field", "func": "tink-daead(some-key)"},
                {"name": "rule-3", "pattern": "dict-field-sid", "func": "map-sid(some-key)"},
                {"name": "rule-4", "pattern": "class-field", "func": "tink-daead(some-key)"},
                {"name": "rule-5", "pattern": "class-field-sid", "func": "map-sid(some-key)"},
            ]
        }
    )
