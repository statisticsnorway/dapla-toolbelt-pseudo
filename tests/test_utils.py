from datetime import date
from unittest.mock import Mock

import polars as pl
import pytest
import json
from gcsfs.core import GCSFile
from google.auth.exceptions import DefaultCredentialsError

from dapla_pseudo.constants import PseudoFunctionTypes
from dapla_pseudo.constants import PseudoOperation
from dapla_pseudo.exceptions import FileInvalidError
from dapla_pseudo.exceptions import MimetypeNotSupportedError
from dapla_pseudo.exceptions import NoFileExtensionError
from dapla_pseudo.utils import build_pseudo_field_request
from dapla_pseudo.utils import convert_to_date
from dapla_pseudo.utils import find_multipart_obj
from dapla_pseudo.utils import get_content_type_from_file
from dapla_pseudo.utils import get_file_data_from_dataset
from dapla_pseudo.utils import get_file_format_from_file_name
from dapla_pseudo.v1.models.api import PseudoFieldRequest, FieldMatch
from dapla_pseudo.v1.models.core import Mimetypes
from dapla_pseudo.v1.models.core import PseudoFunction
from dapla_pseudo.v1.models.core import PseudoRule
from dapla_pseudo.v1.models.core import RedactKeywordArgs
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


@pytest.mark.parametrize("supported_mimetype", Mimetypes.__members__.keys())
def test_get_content_type_from_file(supported_mimetype: str) -> None:
    file_extension = supported_mimetype.lower()
    file_handle = open(f"{TEST_FILE_PATH}/test.{file_extension}", mode="rb")
    content_type = get_content_type_from_file(file_handle)
    assert content_type.name == supported_mimetype


def test_get_content_type_from_gcs_file() -> None:
    mock_gcs_file_handle = Mock(spec=GCSFile)
    mock_gcs_file_handle.full_name = "gs://dummy.json"

    content_type = get_content_type_from_file(mock_gcs_file_handle)
    assert content_type.name == "JSON"


def test_get_content_type_from_file_unsupported_mimetype() -> None:
    file_handle = open(f"{TEST_FILE_PATH}/test.xml", mode="rb")
    with pytest.raises(MimetypeNotSupportedError):
        get_content_type_from_file(file_handle)


def test_get_file_data_from_file_not_a_file() -> None:
    path = f"{TEST_FILE_PATH}/not/a/file.json"
    with pytest.raises(FileNotFoundError):
        get_file_data_from_dataset(path)


def test_get_file_data_from_file_no_file_extension() -> None:
    path = f"{TEST_FILE_PATH}/file_no_extension"

    with pytest.raises(NoFileExtensionError):
        get_file_data_from_dataset(path)


def test_get_file_data_from_file_empty_file() -> None:
    path = f"{TEST_FILE_PATH}/empty_file"

    with pytest.raises(FileInvalidError):
        get_file_data_from_dataset(path)


@pytest.mark.parametrize("file_format", Mimetypes.__members__.keys())
def test_get_file_data_from_file(file_format: str) -> None:
    # Test reading all supported file extensions
    get_file_data_from_dataset(f"{TEST_FILE_PATH}/test.{file_format.lower()}")


def test_get_file_data_from_invalid_gcs_file() -> None:
    invalid_gcs_path = "gs://invalid/path.json"
    with pytest.raises((FileNotFoundError, DefaultCredentialsError)):
        get_file_data_from_dataset(invalid_gcs_path)


def test_get_file_data_from_polars_dataset() -> None:
    df = pl.DataFrame()
    _, mime_type = get_file_data_from_dataset(df)
    assert mime_type.name == "ZIP"


def test_build_pseudo_field_request() -> None:
    data = [{"foo": "bar", "struct": {"foo": "baz"}}]
    df = pl.DataFrame(data)
    df_dict = json.loads(df.write_json())
    rules = [
        PseudoRule.from_json(
            '{"name":"my-rule","pattern":"*foo","func":"redact(placeholder=#)"}'
        )
    ]
    requests = build_pseudo_field_request(PseudoOperation.PSEUDONYMIZE, df_dict, rules)

    assert requests == [
        PseudoFieldRequest(
            pseudo_func=PseudoFunction(
                function_type=PseudoFunctionTypes.REDACT,
                kwargs=RedactKeywordArgs(placeholder="#"),
            ),
            name="/foo",
            values=["bar"],
        ),
        PseudoFieldRequest(
            pseudo_func=PseudoFunction(
                function_type=PseudoFunctionTypes.REDACT,
                kwargs=RedactKeywordArgs(placeholder="#"),
            ),
            name="/struct/foo",
            values=["baz"],
        ),
    ]
