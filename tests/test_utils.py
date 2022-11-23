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
