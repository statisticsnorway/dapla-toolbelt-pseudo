from unittest.mock import ANY
from unittest.mock import AsyncMock
from unittest.mock import Mock
from unittest.mock import patch

import pytest
import pytest_asyncio
import requests
from aiohttp import ClientResponse
from aiohttp import ClientResponseError
from aiohttp import RequestInfo
from aiohttp_retry.client import _RequestContext
from pytest_mock import MockerFixture

from dapla_pseudo import PseudoClient
from dapla_pseudo.constants import TIMEOUT_DEFAULT
from dapla_pseudo.constants import PseudoFunctionTypes
from dapla_pseudo.v1.models.api import PseudoFieldRequest
from dapla_pseudo.v1.models.core import DaeadKeywordArgs
from dapla_pseudo.v1.models.core import PseudoFunction
from dapla_pseudo.v1.models.core import PseudoKeyset

pytest_plugins = ("pytest_asyncio",)


PKG = "dapla_pseudo.v1.client"


@pytest_asyncio.fixture()
def test_client() -> PseudoClient:
    base_url = "https://mocked.dapla-pseudo-service"
    auth_token = "some-auth-token"
    return PseudoClient(pseudo_service_url=base_url, auth_token=auth_token)


@pytest.mark.asyncio
async def test_post_to_field_endpoint_success(
    test_client: PseudoClient, mocker: MockerFixture
) -> None:
    mock_request_context = AsyncMock(spec=_RequestContext)
    mock_response = AsyncMock(spec=ClientResponse)
    mock_response.status = 200

    mock_response_content = {
        "data": [1, 2, 3],
        "logs": ["some-log"],
        "metrics": ["some-metric"],
        "datadoc_metadata": {"variables": [{"some_var": "some_arg"}]},
    }
    mock_response.json.return_value = mock_response_content

    mock_request_context.__aenter__.return_value = mock_response

    mocker.patch(f"{PKG}.RetryClient.post", return_value=mock_request_context)

    mock_pseudo_field_request = Mock(spec=PseudoFieldRequest)
    mock_pseudo_field_request.name = "magic"

    results = await test_client.post_to_field_endpoint(
        path="test_path",
        pseudo_requests=[mock_pseudo_field_request],
        timeout=TIMEOUT_DEFAULT,
    )
    resp_name, resp_data, resp_metadata = results[0]

    assert resp_name == mock_pseudo_field_request.name
    assert resp_data == mock_response_content["data"]
    assert resp_metadata.logs == mock_response_content["logs"]
    assert resp_metadata.metrics == mock_response_content["metrics"]
    assert resp_metadata.datadoc == mock_response_content["datadoc_metadata"]["variables"]  # type: ignore[index]


@pytest.mark.asyncio
async def test_post_to_field_endpoint_failure(
    test_client: PseudoClient, mocker: MockerFixture
) -> None:
    mock_request_context = AsyncMock(spec=_RequestContext)
    mock_response = AsyncMock(spec=ClientResponse)
    mock_response.status = 400
    mock_response.raise_for_status.side_effect = ClientResponseError(
        Mock(spec=RequestInfo), Mock(), message="Mocked HTTP error"
    )
    mock_response.headers = AsyncMock()
    mock_response.text = AsyncMock()

    mock_request_context.__aenter__.return_value = mock_response

    mock_post = mocker.patch(
        f"{PKG}.RetryClient.post", return_value=mock_request_context
    )

    mock_pseudo_field_request = Mock(spec=PseudoFieldRequest)
    mock_pseudo_field_request.name = "magic"

    with pytest.raises(ClientResponseError):
        await test_client.post_to_field_endpoint(
            path="test_path",
            pseudo_requests=[mock_pseudo_field_request],
            timeout=TIMEOUT_DEFAULT,
        )
    mock_post.assert_called_once()
    mock_response.raise_for_status.assert_called_once()


@pytest.mark.asyncio
async def test_post_to_field_endpoint_serialization(
    test_client: PseudoClient, mocker: MockerFixture
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
        pseudo_func=pseudo_func, name="", pattern="", values=[], keyset=keyset
    )

    mock_post = mocker.patch(f"{PKG}.RetryClient.post", return_value=AsyncMock())

    await test_client.post_to_field_endpoint(
        path="test_path",
        pseudo_requests=[pseudo_field_request],
        timeout=TIMEOUT_DEFAULT,
    )
    expected_json = {"request": pseudo_field_request.model_dump(by_alias=True)}

    mock_post.assert_called_once_with(
        url="https://mocked.dapla-pseudo-service/test_path",
        headers={
            "Authorization": "Bearer some-auth-token",
            "Content-Type": "application/json",
            "X-Correlation-Id": ANY,
        },
        json=expected_json,
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
        stream=True,
        timeout=TIMEOUT_DEFAULT,
    )
