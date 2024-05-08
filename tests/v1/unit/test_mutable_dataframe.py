import polars as pl

from dapla_pseudo.v1.models.core import PseudoRule
from dapla_pseudo.v1.mutable_dataframe import MutableDataFrame
from dapla_pseudo.v1.mutable_dataframe import _glob_matches


def test_rule_matching() -> None:
    assert _glob_matches("fnr", "**fnr")
    assert _glob_matches("fnr", "fnr")
    assert _glob_matches("fnr", "fnr*")
    assert _glob_matches("identifier/fnr", "*/fnr")
    assert not _glob_matches("some/identifier/fnr", "*/fnr")
    assert _glob_matches("some/identifier/fnr", "**/fnr")
    assert _glob_matches("identifier/fnr", "identifier/fnr")
    assert _glob_matches("person_info/fnr", "**/fnr")
    assert _glob_matches("person_info/fnr", "**person_info/fnr")
    assert not _glob_matches("identifier/fnr", "fnr")


def test_traverse_dataframe_dict() -> None:
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
    df.match_rules(rules)
    matched_fields = list(df.match_rules(rules))
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
    df.update(matched_fields[0], ["#", "#", "#"])
    df.update(matched_fields[1], ["#", "#", "#"])
    modified_df = df.to_polars()
    print(modified_df)
    # Check that the original dataframe_dict has been changed
    assert modified_df["fnr"][0] == "#"
    assert modified_df["identifiers"][0]["fnr"] == "#"
