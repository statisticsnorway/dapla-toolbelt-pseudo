from datetime import date

import pytest
from dapla_pseudo.exceptions import NoFileExtensionError

from dapla_pseudo.utils import convert_to_date, find_multipart_obj, get_file_format_from_file_name
from dapla_pseudo.v1.supported_file_format import SupportedFileFormat


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
    assert file_format == SupportedFileFormat.CSV


def test_get_file_format_from_file_name_failed() -> None:
    with pytest.raises(NoFileExtensionError):
        file_format = get_file_format_from_file_name("test")
