from unittest.mock import ANY
from unittest.mock import Mock
from unittest.mock import patch

import pytest
import requests

from dapla_pseudo import PseudoClient
from dapla_pseudo.constants import TIMEOUT_DEFAULT
from dapla_pseudo.constants import PseudoFunctionTypes
from dapla_pseudo.v1.models.api import PseudoFieldRequest
from dapla_pseudo.v1.models.api import PseudoFileRequest
from dapla_pseudo.v1.models.core import DaeadKeywordArgs
from dapla_pseudo.v1.models.core import PseudoFunction
from dapla_pseudo.v1.models.core import PseudoKeyset

PKG = "dapla_pseudo.v1.client"


@pytest.fixture
def test_client() -> PseudoClient:
    base_url = "https://mocked.dapla-pseudo-service"
    auth_token = "some-auth-token"
    return PseudoClient(pseudo_service_url=base_url, auth_token=auth_token)


@patch("requests.post")
def test_post_to_field_endpoint_success(
    mock_post: Mock, test_client: PseudoClient
) -> None:
    mock_response = Mock(spec=requests.Response)
    mock_response.status_code = 200
    mock_pseudo_field_request = Mock(spec=PseudoFieldRequest)
    mock_post.return_value = mock_response

    response = test_client._post_to_field_endpoint(
        path="test_path",
        pseudo_field_request=mock_pseudo_field_request,
        timeout=TIMEOUT_DEFAULT,
    )

    assert response == mock_response
    mock_post.assert_called_once()


@patch("requests.post")
def test_post_to_field_endpoint_failure(
    mock_post: Mock, test_client: PseudoClient
) -> None:
    mock_response = Mock(spec=requests.Response)
    mock_response.status_code = 400
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
        "Mocked HTTP error", response=requests.Response()
    )
    mock_response.headers = ANY
    mock_response.text = ANY
    mock_post.return_value = mock_response

    mock_pseudo_request = Mock(spec=PseudoFileRequest)

    with pytest.raises(requests.exceptions.HTTPError):
        test_client._post_to_field_endpoint(
            path="test_path",
            pseudo_field_request=mock_pseudo_request,
            timeout=TIMEOUT_DEFAULT,
        )
    mock_post.assert_called_once()
    mock_response.raise_for_status.assert_called_once()


@patch("requests.post")
def test_post_to_field_endpoint_serialization(
    _mock_post: Mock, test_client: PseudoClient
) -> None:
    keyset = PseudoKeyset(
        encrypted_keyset="test_enc_keyset",
        keyset_info={"primaryKeyId": "test_primary_key_id"},
        kek_uri="test_uri",
    )
    pseudo_func = PseudoFunction(
        function_type=PseudoFunctionTypes.DAEAD, kwargs=DaeadKeywordArgs()
    )
    pseudo_field_request = PseudoFieldRequest(
        pseudo_func=pseudo_func, name="", values=[], keyset=keyset
    )

    test_client._post_to_field_endpoint(
        path="test_path",
        pseudo_field_request=pseudo_field_request,
        timeout=TIMEOUT_DEFAULT,
    )
    expected_json = {"request": pseudo_field_request.model_dump_json(by_alias=True)}

    _mock_post.assert_called_once_with(
        url="https://mocked.dapla-pseudo-service/test_path",
        headers={
            "Authorization": "Bearer some-auth-token",
            "Content-Type": "application/json",
            "X-Correlation-Id": ANY,
        },
        json=expected_json,
        stream=False,
        timeout=TIMEOUT_DEFAULT,
    )


@patch("requests.post")
def test_successful_post_to_sid_endpoint(
    mock_post: Mock, test_client: PseudoClient
) -> None:
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
        headers={
            "Authorization": "Bearer some-auth-token",
            "X-Correlation-Id": ANY,
        },
        json=expected_json,
        stream=False,
        timeout=TIMEOUT_DEFAULT,
    )
