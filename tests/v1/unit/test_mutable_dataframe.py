import polars as pl

from dapla_pseudo.constants import PseudoFunctionTypes
from dapla_pseudo.v1.models.core import DaeadKeywordArgs
from dapla_pseudo.v1.models.core import PseudoFunction
from dapla_pseudo.v1.models.core import PseudoRule
from dapla_pseudo.v1.mutable_dataframe import MutableDataFrame
from dapla_pseudo.v1.mutable_dataframe import _glob_matches


def test_rule_matching() -> None:
    assert _glob_matches("fnr", "**fnr")
    assert _glob_matches("fnr", "fnr")
    assert _glob_matches("fnr", "Fnr")
    assert _glob_matches("fnr", "fnr*")
    assert _glob_matches("identifier/fnr", "*/fnr")
    assert _glob_matches("identifier/fnr", "*/{id,*nr}")
    assert _glob_matches("identifier/fnr", "*/{dnr,fnr}")
    assert not _glob_matches("some/identifier/fnr", "*/fnr")
    assert _glob_matches("some/identifier/fnr", "**/fnr")
    assert _glob_matches("identifier/fnr", "identifier/fnr")
    assert _glob_matches("person_info/fnr", "**/fnr")
    assert _glob_matches("person_info/fnr", "**person_info/fnr")
    assert not _glob_matches("identifier/fnr", "fnr")


def test_match_dataframe_dict_for_repseudo() -> None:
    data = [{"foo": "bar"}]
    source_rules = [
        PseudoRule.from_json(
            '{"name":"foo-rule","pattern":"**/foo","func":"daead(keyId=old-key)"}'
        )
    ]
    target_rules = [
        PseudoRule.from_json(
            '{"name":"foo-rule","pattern":"**/foo","func":"daead(keyId=new-key)"}'
        )
    ]
    df = MutableDataFrame(pl.DataFrame(data))
    df.match_rules(source_rules, target_rules)
    matched_fields = df.get_matched_fields()
    assert len(matched_fields) == 1
    assert matched_fields[0].path == "foo"
    assert matched_fields[0].func == PseudoFunction(
        function_type=PseudoFunctionTypes.DAEAD,
        kwargs=DaeadKeywordArgs(key_id="old-key"),
    )
    assert matched_fields[0].target_func == PseudoFunction(
        function_type=PseudoFunctionTypes.DAEAD,
        kwargs=DaeadKeywordArgs(key_id="new-key"),
    )


def test_match_nested_dataframe_dict() -> None:
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
            "identifiers": {"fnr": "06097048531"},
            "fornavn": "Gunnar",
        },
        {
            "identifiers": {"fnr": "02812289295"},
            "fnr": "02812289295",
            "fornavn": "Kristoffer",
        },
    ]
    rules = [
        PseudoRule.from_json(
            '{"name":"my-rule","pattern":"**/fnr","func":"redact(placeholder=#)"}'
        )
    ]
    df = MutableDataFrame(pl.DataFrame(data))
    df.match_rules(rules, None)
    matched_fields = df.get_matched_fields()
    assert len(matched_fields) == 2
    assert matched_fields[0].path == "identifiers/fnr"
    assert matched_fields[1].path == "fnr"
    assert matched_fields[0].col["name"] == "fnr"
    assert matched_fields[0].col["values"] == [
        "11854898347",
        "06097048531",
        "02812289295",
    ]
    # Test updating the columns in the MutableDataFrame
    df.update("identifiers/fnr", ["#", "#", "#"])
    df.update("fnr", ["#", "#", "#"])
    modified_df = df.to_polars()
    print(modified_df)
    # Check that the original dataframe_dict has been changed
    assert modified_df["fnr"][0] == "#"
    assert modified_df["identifiers"][0]["fnr"] == "#"


def test_traverse_list_of_struct() -> None:
    data = [
        {
            "identifiers": [
                {"type": "fnr", "value": "11854898347"},
            ],
        },
        {
            "identifiers": [
                {"type": "fnr", "value": "06097048531"},
            ],
        },
    ]
    rules = [
        PseudoRule.from_json(
            '{"name":"nick-rule","pattern":"**/value","func":"redact(placeholder=#)"}'
        )
    ]
    df = MutableDataFrame(pl.DataFrame(data))
    df.match_rules(rules, None)
    matched_fields = df.get_matched_fields()
    print(f"Match field metrics: {df.matched_fields_metrics}")
    # This shows the lack of support for matching on list of dicts
    # We get two matched_fields instead of one
    assert len(matched_fields) == 2
    assert matched_fields[0].path == "identifiers[0]/value"
    assert matched_fields[0].col["name"] == "value"
    assert matched_fields[0].col["values"] == ["11854898347"]
    assert matched_fields[1].path == "identifiers[1]/value"
    assert matched_fields[1].col["name"] == "value"
    assert matched_fields[1].col["values"] == ["06097048531"]
    # Ideally, we should get just one, with the following valued
    # assert matched_fields[0].col["values"] == ["11854898347", "06097048531"]
