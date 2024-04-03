from unittest.mock import Mock

import pytest
from gcsfs.core import GCSFile

from dapla_pseudo.exceptions import MimetypeNotSupportedError
from dapla_pseudo.utils import get_content_type_from_file
from dapla_pseudo.v1.models.core import Mimetypes

PKG = "dapla_pseudo.v1.builder_pseudo"
TEST_FILE_PATH = "tests/v1/test_files"


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
