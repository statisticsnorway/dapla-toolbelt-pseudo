import polars as pl

from dapla_pseudo.constants import PseudoFunctionTypes
from dapla_pseudo.v1.models.core import DaeadKeywordArgs
from dapla_pseudo.v1.models.core import PseudoFunction
from dapla_pseudo.v1.models.core import PseudoRule
from dapla_pseudo.v1.mutable_dataframe import MutableDataFrame


def test_match_dataframe_dict_for_repseudo() -> None:
    data = [{"foo": "bar"}]
    source_rules = [
        PseudoRule.from_json(
            '{"name":"foo-rule","pattern":"**/foo", "path":"foo", "func":"daead(keyId=old-key)"}'
        )
    ]
    target_rules = [
        PseudoRule.from_json(
            '{"name":"foo-rule","pattern":"**/foo","path":"foo", "func":"daead(keyId=new-key)"}'
        )
    ]
    df = MutableDataFrame(pl.DataFrame(data), hierarchical=True)
    df.match_rules(source_rules, target_rules)
    matched_fields = df.get_matched_fields()

    path = "foo"
    assert len(matched_fields) == 1
    assert matched_fields[path].path == "foo"
    assert matched_fields[path].func == PseudoFunction(
        function_type=PseudoFunctionTypes.DAEAD,
        kwargs=DaeadKeywordArgs(key_id="old-key"),
    )
    assert matched_fields[path].target_func == PseudoFunction(
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
            '{"name":"my-rule","pattern":"**/fnr", "path":"identifiers/fnr", "func":"redact(placeholder=#)"}'
        ),
        PseudoRule.from_json(
            '{"name":"my-rule","pattern":"**/fnr", "path":"fnr", "func":"redact(placeholder=#)"}'
        ),
    ]
    df = MutableDataFrame(pl.DataFrame(data), hierarchical=True)
    df.match_rules(rules, None)
    matched_fields = df.get_matched_fields()
    assert len(matched_fields) == 2

    matched_path_1 = "identifiers/fnr"
    matched_path_2 = "fnr"

    assert matched_fields[matched_path_1].path == "identifiers/fnr"
    assert matched_fields[matched_path_2].path == "fnr"
    assert matched_fields[matched_path_1].col["name"] == "fnr"  # type: ignore
    assert matched_fields[matched_path_1].col["values"] == [  # type: ignore
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
            '{"name":"nick-rule","pattern":"**/value", "path":"identifiers/value", "func":"redact(placeholder=#)"}'
        )
    ]
    df = MutableDataFrame(pl.DataFrame(data), hierarchical=True)
    df.match_rules(rules, None)
    matched_fields = df.get_matched_fields()
    print(f"Match field metrics: {df.matched_fields_metrics}")
    # This shows the lack of support for matching on list of dicts
    # We get two matched_fields instead of one
    assert len(matched_fields) == 2

    path_1 = "identifiers[0]/value"
    match_1 = matched_fields[path_1]
    assert isinstance(match_1.col, dict)
    assert match_1.path == "identifiers[0]/value"
    assert match_1.col["name"] == "value"
    assert match_1.col["values"] == ["11854898347"]

    path_2 = "identifiers[1]/value"
    match_2 = matched_fields[path_2]
    assert isinstance(match_2.col, dict)
    assert match_2.path == "identifiers[1]/value"
    assert match_2.col["name"] == "value"
    assert match_2.col["values"] == ["06097048531"]
    # Ideally, we should get just one, with the following valued
    # assert matched_fields[0].col["values"] == ["11854898347", "06097048531"]


def test_traverse_list_inner() -> None:
    data = [
        {
            "identifiers": [
                {"type": "fnr", "values": ["11854898347", "99600884572"]},
            ],
        },
        {
            "identifiers": [
                {"type": "fnr", "values": ["06097048531", "59900946537"]},
            ],
        },
    ]
    rules = [
        PseudoRule.from_json(
            '{"name":"nick-rule","pattern":"**/values", "path":"identifiers/values", "func":"redact(placeholder=#)"}'
        )
    ]

    df = MutableDataFrame(pl.DataFrame(data), hierarchical=True)
    df.match_rules(rules, None)
    matched_fields = df.get_matched_fields()
    print(f"Match field metrics: {df.matched_fields_metrics}")
    assert len(matched_fields) == 2

    path_1 = "identifiers[0]/values"
    match_1 = matched_fields[path_1]
    assert isinstance(match_1.col, dict)
    assert match_1.path == path_1
    assert match_1.col["name"] == ""
    assert match_1.col["values"] == ["11854898347", "99600884572"]

    path_2 = "identifiers[1]/values"
    match_2 = matched_fields[path_2]
    assert isinstance(match_2.col, dict)
    assert match_2.path == path_2
    assert match_2.col["name"] == ""
    assert match_2.col["values"] == ["06097048531", "59900946537"]
