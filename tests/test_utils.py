from datetime import date
from unittest.mock import Mock

import pytest
from gcsfs.core import GCSFile

from dapla_pseudo.exceptions import MimetypeNotSupportedError
from dapla_pseudo.exceptions import NoFileExtensionError
from dapla_pseudo.utils import convert_to_date
from dapla_pseudo.utils import find_multipart_obj
from dapla_pseudo.utils import get_content_type_from_file
from dapla_pseudo.utils import get_file_format_from_file_name
from dapla_pseudo.v1.models.core import Mimetypes
from dapla_pseudo.v1.supported_file_format import SupportedOutputFileFormat

TEST_FILE_PATH = "tests/v1/test_files"


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
