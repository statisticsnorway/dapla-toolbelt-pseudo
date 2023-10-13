from datetime import date

import pytest

from dapla_pseudo import utils


def test_find_multipart_obj() -> None:
    obj1 = open("tests/data/personer.json", "rb")
    obj2 = '{"foo": "bar"}'
    multipart_objects = {
        ("data", ("data.json", obj1, "application/json")),
        ("request", (None, obj2, "application/json")),
    }

    assert utils.find_multipart_obj("data", multipart_objects) == obj1
    assert utils.find_multipart_obj("request", multipart_objects) == obj2
    assert utils.find_multipart_obj("bogus", multipart_objects) is None


def test_convert_to_date_valid() -> None:
    valid_date_str = "2023-05-04"
    assert utils.convert_to_date(valid_date_str) == date.fromisoformat(valid_date_str)


def test_convert_to_date_invalid_date_str() -> None:
    invalid_date_str = "04-05-2023"
    with pytest.raises(ValueError):
        utils.convert_to_date(invalid_date_str)


def test_convert_to_date_with_date_type() -> None:
    valid_date_type = date.fromisoformat("2023-05-04")
    assert utils.convert_to_date(valid_date_type) == date.fromisoformat("2023-05-04")


def test_convert_to_date_with_none() -> None:
    assert utils.convert_to_date(None) is None
