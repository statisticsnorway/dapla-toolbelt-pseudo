from datetime import date

import polars as pl
import pytest

from dapla_pseudo.constants import PseudoFunctionTypes
from dapla_pseudo.constants import PseudoOperation
from dapla_pseudo.exceptions import NoFileExtensionError
from dapla_pseudo.utils import build_pseudo_field_request
from dapla_pseudo.utils import convert_to_date
from dapla_pseudo.utils import find_multipart_obj
from dapla_pseudo.utils import get_file_format_from_file_name
from dapla_pseudo.v1.models.api import PseudoFieldRequest
from dapla_pseudo.v1.models.api import RepseudoFieldRequest
from dapla_pseudo.v1.models.core import DaeadKeywordArgs
from dapla_pseudo.v1.models.core import PseudoFunction
from dapla_pseudo.v1.models.core import PseudoRule
from dapla_pseudo.v1.models.core import RedactKeywordArgs
from dapla_pseudo.v1.mutable_dataframe import MutableDataFrame
from dapla_pseudo.v1.supported_file_format import SupportedOutputFileFormat

TEST_FILE_PATH = "tests/v1/unit/test_files"


def test_find_multipart_obj() -> None:
    obj1 = open("tests/data/personer.json", "rb")
    obj2 = '{"foo": "bar"}'
    multipart_objects = {
        ("data", ("data.json", obj1, "application/json")),
        ("request", (None, obj2, "application/json")),
    }

    assert find_multipart_obj("data", multipart_objects) == obj1
    assert find_multipart_obj("request", multipart_objects) == obj2
    assert find_multipart_obj("bogus", multipart_objects) is None


def test_convert_to_date_valid() -> None:
    valid_date_str = "2023-05-04"
    assert convert_to_date(valid_date_str) == date.fromisoformat(valid_date_str)


def test_convert_to_date_invalid_date_str() -> None:
    invalid_date_str = "04-05-2023"
    with pytest.raises(ValueError):
        convert_to_date(invalid_date_str)


def test_convert_to_date_with_date_type() -> None:
    valid_date_type = date.fromisoformat("2023-05-04")
    assert convert_to_date(valid_date_type) == date.fromisoformat("2023-05-04")


def test_convert_to_date_with_none() -> None:
    assert convert_to_date(None) is None


def test_get_file_format_from_file_name_successful() -> None:
    file_format = get_file_format_from_file_name("test.csv")
    assert file_format == SupportedOutputFileFormat.CSV


def test_get_file_format_from_file_name_failed() -> None:
    with pytest.raises(NoFileExtensionError):
        get_file_format_from_file_name("test")


def test_build_pseudo_field_request() -> None:
    data = [
        {"foo": "bar", "struct": {"foo": "baz"}},
        {"foo": "bad", "struct": {"foo": None}},
    ]
    df = MutableDataFrame(pl.DataFrame(data), hierarchical=True)
    rules = [
        PseudoRule.from_json(
            '{"name":"my-rule","pattern":"**/foo","path":"foo","func":"redact(placeholder=#)"}'
        ),
        PseudoRule.from_json(
            '{"name":"my-rule","pattern":"**/foo","path":"struct/foo","func":"redact(placeholder=#)"}'
        ),
    ]
    requests = build_pseudo_field_request(PseudoOperation.PSEUDONYMIZE, df, rules)
    assert requests[0] == PseudoFieldRequest(
        pseudo_func=PseudoFunction(
            function_type=PseudoFunctionTypes.REDACT,
            kwargs=RedactKeywordArgs(placeholder="#"),
        ),
        name="foo",
        pattern="**/foo",
        values=["bar", "bad"],
    )
    assert requests[1] == PseudoFieldRequest(
        pseudo_func=PseudoFunction(
            function_type=PseudoFunctionTypes.REDACT,
            kwargs=RedactKeywordArgs(placeholder="#"),
        ),
        name="struct[0]/foo",
        pattern="**/foo",
        values=["baz"],
    )


def test_build_repseudo_field_request() -> None:
    data = [
        {"foo": "bar", "struct": {"foo": "baz"}},
        {"foo": "bad", "struct": {"foo": None}},
    ]
    df = MutableDataFrame(pl.DataFrame(data), hierarchical=True)
    source_rules = [
        PseudoRule.from_json(
            '{"name":"my-rule","pattern":"**/foo","path":"foo","func":"daead(keyId=old-key)"}'
        ),
        PseudoRule.from_json(
            '{"name":"my-rule","pattern":"**/foo","path":"struct/foo","func":"daead(keyId=old-key)"}'
        ),
    ]
    target_rules = [
        PseudoRule.from_json(
            '{"name":"my-rule","pattern":"**/foo","path":"foo","func":"daead(keyId=new-key)"}'
        ),
        PseudoRule.from_json(
            '{"name":"my-rule","pattern":"**/foo","path":"struct/foo","func":"daead(keyId=new-key)"}'
        ),
    ]
    requests = build_pseudo_field_request(
        PseudoOperation.REPSEUDONYMIZE, df, source_rules, target_rules=target_rules
    )

    assert requests[0] == RepseudoFieldRequest(
        source_pseudo_func=PseudoFunction(
            function_type=PseudoFunctionTypes.DAEAD,
            kwargs=DaeadKeywordArgs(key_id="old-key"),
        ),
        target_pseudo_func=PseudoFunction(
            function_type=PseudoFunctionTypes.DAEAD,
            kwargs=DaeadKeywordArgs(key_id="new-key"),
        ),
        name="foo",
        pattern="**/foo",
        values=["bar", "bad"],
    )
    assert requests[1] == RepseudoFieldRequest(
        source_pseudo_func=PseudoFunction(
            function_type=PseudoFunctionTypes.DAEAD,
            kwargs=DaeadKeywordArgs(key_id="old-key"),
        ),
        target_pseudo_func=PseudoFunction(
            function_type=PseudoFunctionTypes.DAEAD,
            kwargs=DaeadKeywordArgs(key_id="new-key"),
        ),
        name="struct[0]/foo",
        pattern="**/foo",
        values=["baz"],
    )


def test_build_pseudo_field_request_hierarchical_batching() -> None:
    data = [
        {"struct": {"foo": "baz"}},
        {"struct": {"foo": "qux"}},
    ]
    df = MutableDataFrame(pl.DataFrame(data), hierarchical=True)
    rules = [
        PseudoRule.from_json(
            '{"name":"my-rule","pattern":"**/foo","path":"struct/foo","func":"daead(keyId=ssb-common-key-1)"}'
        )
    ]

    requests = build_pseudo_field_request(PseudoOperation.PSEUDONYMIZE, df, rules)

    assert len(requests) == 1
    assert requests[0].name == "struct/foo"
    assert requests[0].pattern == "**/foo"
    assert requests[0].values == ["baz", "qux"]

    # Ensure batched responses can be scattered back to concrete paths.
    df.update("struct/foo", ["#", "#"])
    modified_df = df.to_polars()
    assert modified_df["struct"][0]["foo"] == "#"
    assert modified_df["struct"][1]["foo"] == "#"
