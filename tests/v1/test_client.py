import json
from typing import BinaryIO
from unittest import mock
from unittest.mock import ANY
from unittest.mock import Mock
from unittest.mock import patch

import pytest
import requests

from dapla_pseudo import PseudoClient
from dapla_pseudo.constants import TIMEOUT_DEFAULT
from dapla_pseudo.v1.models import Mimetypes
from dapla_pseudo.v1.models import PseudoKeyset
from dapla_pseudo.v1.models import PseudonymizeFileRequest


@pytest.fixture
def test_client() -> PseudoClient:
    base_url = "https://mocked.dapla-pseudo-service"
    auth_token = "some-auth-token"
    return PseudoClient(pseudo_service_url=base_url, auth_token=auth_token)


def test_export_dataset(test_client: PseudoClient) -> None:
    request_json = json.dumps(
        {
            "sourceDataset": {"root": "gs://some-bucket", "path": "/path/to/some/data", "version": "123"},
            "targetContentName": "blah",
            "targetContentType": "application/json",
            "targetPassword": "kensentme",
            "depseudonymize": True,
            "pseudoRules": [
                {"name": "rule-1", "pattern": "**/{*Fnr,*Id}", "func": "tink-daead(ssb-common-key-1)"},
                {"name": "rule-2", "pattern": "**/path/to/ignorable/stuff/*", "func": "redact(***)"},
            ],
        }
    )

    with mock.patch("requests.post") as patched:
        test_client.export_dataset(request_json=request_json)
        patched.assert_called_once()
        arg = patched.call_args.kwargs

        assert arg["url"] == f"{test_client.pseudo_service_url}/export"
        assert arg["headers"] == {
            "Authorization": f"Bearer {test_client.static_auth_token}",
            "Content-Type": "application/json",
        }
        assert arg["data"] == request_json


@patch("requests.post")
def test_post_to_field_endpoint_success(mock_post: Mock, test_client: PseudoClient) -> None:
    mock_response = Mock(spec=requests.Response)
    mock_response.status_code = 200
    mock_response.raise_for_status.return_value = None

    mock_post.return_value = mock_response
    response = test_client._post_to_field_endpoint(
        path="test_path",
        field_name="test_field",
        values=["value1", "value2"],
        pseudo_func=None,
        timeout=TIMEOUT_DEFAULT,
    )

    assert response == mock_response
    mock_post.assert_called_once()


@patch("requests.post")
def test_post_to_file_endpoint_success(mock_post: Mock, test_client: PseudoClient) -> None:
    mock_response = Mock(spec=requests.Response)
    mock_response.status_code = 200
    mock_response.raise_for_status.return_value = None

    mock_pseudo_request = Mock(spec=PseudonymizeFileRequest)
    mock_pseudo_request.to_json.return_value = Mock()

    mock_post.return_value = mock_response
    response = test_client._post_to_file_endpoint(
        path="test_path",
        request=mock_pseudo_request,
        data=Mock(spec=BinaryIO),
        name="test_name",
        content_type=Mimetypes.JSON,
        timeout=TIMEOUT_DEFAULT,
        stream=True,
    )

    assert response == mock_response
    mock_post.assert_called_once()


@patch("requests.post")
def test__post_to_field_endpoint_failure(mock_post: Mock, test_client: PseudoClient) -> None:
    mock_response = Mock(spec=requests.Response)
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
        "Mocked HTTP error", response=requests.Response()
    )
    mock_post.return_value = mock_response

    with pytest.raises(requests.exceptions.HTTPError):
        test_client._post_to_field_endpoint(
            path="test_path",
            field_name="test_field",
            values=["value1", "value2"],
            pseudo_func=None,
            timeout=TIMEOUT_DEFAULT,
        )
    mock_post.assert_called_once()
    mock_response.raise_for_status.assert_called_once()


@patch("requests.post")
def test_post_to_file_endpoint_failure(mock_post: Mock, test_client: PseudoClient) -> None:
    mock_response = Mock(spec=requests.Response)
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
        "Mocked HTTP error", response=requests.Response()
    )
    mock_post.return_value = mock_response

    mock_pseudo_request = Mock(spec=PseudonymizeFileRequest)
    mock_pseudo_request.to_json.return_value = Mock()

    mock_post.return_value = mock_response

    with pytest.raises(requests.exceptions.HTTPError):
        test_client._post_to_file_endpoint(
            path="test_path",
            request=mock_pseudo_request,
            data=Mock(spec=BinaryIO),
            name="test_name",
            content_type=Mimetypes.JSON,
            timeout=TIMEOUT_DEFAULT,
            stream=True,
        )

    mock_post.assert_called_once()
    mock_response.raise_for_status.assert_called_once()


@patch("requests.post")
def test_post_to_field_endpoint_with_keyset(_mock_post: Mock, test_client: PseudoClient) -> None:
    keyset = PseudoKeyset(
        encrypted_keyset="test_enc_keyset", keyset_info={"primaryKeyId": "test_primary_key_id"}, kek_uri="test_uri"
    )

    test_client._post_to_field_endpoint(
        path="test_path",
        field_name="test_field",
        values=["value1", "value2"],
        pseudo_func=None,
        timeout=TIMEOUT_DEFAULT,
        keyset=keyset,
    )
    expected_json = {
        "name": ANY,
        "values": ANY,
        "pseudoFunc": ANY,
        "keyset": {
            "kekUri": "test_uri",
            "encryptedKeyset": "test_enc_keyset",
            "keysetInfo": {"primaryKeyId": "test_primary_key_id"},
        },
    }

    _mock_post.assert_called_once_with(
        url="https://mocked.dapla-pseudo-service/test_path",
        headers={"Authorization": "Bearer some-auth-token", "Content-Type": "Mimetypes.JSON"},
        json=expected_json,
        stream=False,
        timeout=30,
    )


@patch("requests.post")
def test_successful_post_to_sid_endpoint(mock_post: Mock, test_client: PseudoClient) -> None:
    mock_response = Mock(spec=requests.Response)
    mock_response.status_code = 200
    mock_response.raise_for_status.return_value = None

    mock_post.return_value = mock_response
    response = test_client._post_to_sid_endpoint(
        path="test_path",
        values=["value1", "value2"],
    )

    expected_json = {"fnrList": ["value1", "value2"]}
    assert response == mock_response
    mock_post.assert_called_once_with(
        url="https://mocked.dapla-pseudo-service/test_path",
        params=None,
        headers={"Authorization": "Bearer some-auth-token"},
        json=expected_json,
        stream=False,
        timeout=30,
    )
