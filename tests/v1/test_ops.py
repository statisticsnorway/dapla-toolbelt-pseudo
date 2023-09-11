import json
from typing import Any
from typing import Sequence

import pandas as pd
import pytest

from dapla_pseudo.v1.models import Field
from dapla_pseudo.v1.models import PseudoConfig
from dapla_pseudo.v1.ops import _dataframe_to_json
from dapla_pseudo.v1.ops import _rules_of


def test_generate_rules_from_single_field_daead() -> None:
    rules = _rules_of(key="some-key", fields=["some-field"], sid_fields=[])
    assert PseudoConfig(rules=rules, keysets=None).model_dump() == \
        {"keysets": None, "rules": [{"name": "rule-1", "pattern": "**/some-field", "func": "daead(keyId=some-key)"}]}
    


def test_generate_rules_from_single_field_fpe() -> None:
    rules = _rules_of(key="papis-common-key-1", fields=["some-field"], sid_fields=[])
    assert PseudoConfig(rules=rules, keysets=None).model_dump() == \
    {"keysets": None, "rules": [{"name": "rule-1", "pattern": "**/some-field", "func": "ff31(keyId=papis-common-key-1,strategy=skip)"}]}


def test_generate_rules_from_multiple_field() -> None:
    rules = _rules_of(key="some-key", fields=["some-field", "another-field", "yet-another-field"], sid_fields=[])
    assert PseudoConfig(rules=rules, keysets=None).model_dump() == \
        {
            "keysets": None,
            "rules": [
                {"name": "rule-1", "pattern": "**/some-field", "func": "daead(keyId=some-key)"},
                {"name": "rule-2", "pattern": "**/another-field", "func": "daead(keyId=some-key)"},
                {"name": "rule-3", "pattern": "**/yet-another-field", "func": "daead(keyId=some-key)"},
            ]
        }


def test_generate_rules_from_hierarchy_field() -> None:
    rules = _rules_of(key="some-key", fields=["path/to/*-field"], sid_fields=[])
    assert PseudoConfig(rules=rules, keysets=None).model_dump() == \
        {"keysets": None, "rules": [{"name": "rule-1", "pattern": "**/path/to/*-field", "func": "daead(keyId=some-key)"}]}
    


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
        sid_fields=[],
    )
    assert PseudoConfig(rules=rules, keysets=None).model_dump() == \
        {
            "keysets": None,
            "rules": [
                {"name": "rule-1", "pattern": "**/string-field", "func": "daead(keyId=some-key)"},
                {"name": "rule-2", "pattern": "dict-field", "func": "daead(keyId=some-key)"},
                {"name": "rule-3", "pattern": "dict-field-sid", "func": "map-sid(keyId=papis-common-key-1)"},
                {"name": "rule-4", "pattern": "class-field", "func": "daead(keyId=some-key)"},
                {"name": "rule-5", "pattern": "class-field-sid", "func": "map-sid(keyId=papis-common-key-1)"},
            ]
        }
    


def test_dataframe_to_json_minimal_call() -> None:
    _dataframe_to_json(pd.DataFrame())


@pytest.mark.parametrize(
    "input_dict,expected_output,fields,sid_fields",
    [
        ({"a": [1, 2, 3]}, [{"a": 1}, {"a": 2}, {"a": 3}], None, None),
        ({"a": [1, 2, 3]}, [{"a": "1"}, {"a": "2"}, {"a": "3"}], ["a"], None),
        ({"a": [1, 2, 3]}, [{"a": "1"}, {"a": "2"}, {"a": "3"}], None, ["a"]),
        ({"a": [1, 2, 3]}, [{"a": "1"}, {"a": "2"}, {"a": "3"}], ["a"], ["a"]),
        ({"a": ["1", "2", "3"]}, [{"a": "1"}, {"a": "2"}, {"a": "3"}], ["a"], None),
    ],
)
def test_dataframe_to_json_type_conversion(
    input_dict: dict[str, Any], expected_output: dict[str, Any], fields: Sequence[str], sid_fields: Sequence[str]
) -> None:
    handle = _dataframe_to_json(pd.DataFrame(input_dict), fields=fields, sid_fields=sid_fields)
    assert expected_output == json.load(handle)


@pytest.mark.parametrize(
    "input_dict,fields,sid_fields",
    [
        ({"a": [1, 2, 3]}, None, ["b"]),
        ({"a": [1, 2, 3]}, ["b"], None),
    ],
)
def test_dataframe_to_json_unknown_field(
    input_dict: dict[str, Any], fields: Sequence[str], sid_fields: Sequence[str]
) -> None:
    with pytest.raises(KeyError):
        _dataframe_to_json(pd.DataFrame(input_dict), fields=fields, sid_fields=sid_fields)
