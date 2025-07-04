from unittest.mock import ANY
from unittest.mock import AsyncMock
from unittest.mock import Mock
from unittest.mock import patch

import pytest
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
from dapla_pseudo.v1.models.api import RawPseudoMetadata
from dapla_pseudo.v1.models.core import DaeadKeywordArgs
from dapla_pseudo.v1.models.core import PseudoFunction
from dapla_pseudo.v1.models.core import PseudoKeyset

pytest_plugins = ("pytest_asyncio",)


PKG = "dapla_pseudo.v1.client"


@pytest.fixture()
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
        "data": ["1", "2", "3"],
        "logs": ["some-log"],
        "metrics": [{"some-metric": 1}],
        "datadoc_metadata": {"variables": [{"some_var": "some_arg"}]},
    }
    mock_response.json.return_value = mock_response_content

    mock_request_context.__aenter__.return_value = mock_response

    mock_post = mocker.patch(
        f"{PKG}.RetryClient.post", return_value=mock_request_context
    )

    mock_pseudo_field_request = Mock(spec=PseudoFieldRequest)
    mock_pseudo_field_request.name = "magic"
    mock_pseudo_field_request.values = ["1", "2", "3"]

    results = await test_client.post_to_field_endpoint(
        path="test_path",
        pseudo_requests=[mock_pseudo_field_request],
        timeout=TIMEOUT_DEFAULT,
    )
    resp_name, resp_data, resp_metadata = results[0]

    assert mock_post.call_count == 1
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
    mock_pseudo_field_request.values = ["1", "2", "3"]

    with pytest.raises(ClientResponseError):
        await test_client.post_to_field_endpoint(
            path="test_path",
            pseudo_requests=[mock_pseudo_field_request],
            timeout=TIMEOUT_DEFAULT,
        )
    assert mock_post.call_count == 1
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


@pytest.mark.asyncio
async def test_post_to_field_endpoint_multiple_partitions(
    mocker: MockerFixture,
) -> None:
    mock_request_context = AsyncMock(spec=_RequestContext)
    mock_response = AsyncMock(spec=ClientResponse)
    mock_response.status = 200
    client = PseudoClient(auth_token="some-token", rows_per_partition="1")

    mock_response_content = {
        "data": ["1"],
        "logs": ["some-log"],
        "metrics": [{"some-metric": 1}],
        "datadoc_metadata": {"variables": [{"magic": "some_arg"}]},
    }
    mock_response.json.return_value = mock_response_content

    mock_request_context.__aenter__.return_value = mock_response

    mock_post = mocker.patch(
        f"{PKG}.RetryClient.post", return_value=mock_request_context
    )

    mock_pseudo_field_request = Mock(spec=PseudoFieldRequest)
    mock_pseudo_field_request.name = "magic"
    mock_pseudo_field_request.values = ["1", "1", "1"]

    results = await client.post_to_field_endpoint(
        path="test_path",
        pseudo_requests=[mock_pseudo_field_request],
        timeout=TIMEOUT_DEFAULT,
    )
    _, resp_data, resp_metadata = results[0]

    expected_json = {
        "data": ["1", "1", "1"],
        "logs": ["some-log"] * 3,
        "metrics": [{"some-metric": 3}],
        "datadoc_metadata": {"variables": [{"magic": "some_arg"}]},
    }

    assert mock_post.call_count == 3
    assert resp_data == expected_json["data"]
    assert resp_metadata.logs == expected_json["logs"]
    assert resp_metadata.metrics == expected_json["metrics"]
    assert (
        resp_metadata.datadoc == expected_json["datadoc_metadata"]["variables"]  # type: ignore[index]
    )


@pytest.mark.asyncio
async def test_post_to_field_endpoint_max_partitions(mocker: MockerFixture) -> None:
    mock_request_context = AsyncMock(spec=_RequestContext)
    mock_response = AsyncMock(spec=ClientResponse)
    mock_response.status = 200
    client = PseudoClient(
        auth_token="some-token", rows_per_partition="1", max_total_partitions="3"
    )

    mock_response_content = {
        "data": ["1"] * 2,
        "logs": ["some-log"] * 2,
        "metrics": [{"some-metric": 2}],
        "datadoc_metadata": {"variables": [{"magic": "some_arg"}]},
    }
    mock_response.json.return_value = mock_response_content

    mock_request_context.__aenter__.return_value = mock_response

    mock_post = mocker.patch(
        f"{PKG}.RetryClient.post", return_value=mock_request_context
    )

    mock_pseudo_field_request = Mock(spec=PseudoFieldRequest)
    mock_pseudo_field_request.name = "magic"
    mock_pseudo_field_request.values = ["1"] * 6

    results = await client.post_to_field_endpoint(
        path="test_path",
        pseudo_requests=[mock_pseudo_field_request],
        timeout=TIMEOUT_DEFAULT,
    )
    _, resp_data, resp_metadata = results[0]

    expected_json = {
        "data": ["1"] * 6,
        "logs": ["some-log"] * 6,
        "metrics": [{"some-metric": 6}],
        "datadoc_metadata": {"variables": [{"magic": "some_arg"}]},
    }

    assert mock_post.call_count == 3
    assert resp_data == expected_json["data"]
    assert resp_metadata.logs == expected_json["logs"]
    assert resp_metadata.metrics == expected_json["metrics"]
    assert (
        resp_metadata.datadoc == expected_json["datadoc_metadata"]["variables"]  # type: ignore[index]
    )


@pytest.mark.asyncio
async def test_post_to_field_endpoint_test_splits() -> None:
    mock_all_responses_content = [
        (
            "magic",
            ["1", "2", "3"],
            RawPseudoMetadata(
                logs=["some-log"],
                metrics=[{"some-metric": 1}],
                datadoc=[{"datadoc_metadata": {"variables": [{"magic": "some_arg"}]}}],
            ),
        ),
        (
            "magic",
            ["1", "2", "3"],
            RawPseudoMetadata(
                logs=["some-log"],
                metrics=[{"some-metric": 1}],
                datadoc=[{"datadoc_metadata": {"variables": [{"magic": "some_arg"}]}}],
            ),
        ),
        (
            "magic",
            ["1", "2", "3"],
            RawPseudoMetadata(
                logs=["some-log"],
                metrics=[{"some-metric": 1}],
                datadoc=[{"datadoc_metadata": {"variables": [{"magic": "some_arg"}]}}],
            ),
        ),
    ]

    mock_pseudo_field_request = Mock(spec=PseudoFieldRequest)
    mock_pseudo_field_request.name = "magic"
    mock_pseudo_field_request.values = ["1", "2", "3"]
    client = PseudoClient(
        rows_per_partition="1",
        auth_token="some-token",
    )

    with patch("asyncio.gather", new_callable=AsyncMock) as mock_gather:
        mock_gather.return_value = mock_all_responses_content

        results = await client.post_to_field_endpoint(
            path="test_path",
            pseudo_requests=[mock_pseudo_field_request],
            timeout=TIMEOUT_DEFAULT,
        )
        _, resp_data, resp_metadata = results[0]

        expected_json = {
            "data": ["1", "2", "3"] * 3,
            "logs": ["some-log"] * 3,
            "metrics": [{"some-metric": 3}],
            "datadoc_metadata": {"variables": [{"magic": "some_arg"}]},
        }

        assert resp_data == expected_json["data"]
        assert resp_metadata.logs == expected_json["logs"]
        assert resp_metadata.metrics == expected_json["metrics"]
        assert (
            resp_metadata.datadoc[0]["datadoc_metadata"]["variables"] == expected_json["datadoc_metadata"]["variables"]  # type: ignore[index]
        )


@pytest.mark.asyncio
async def test_post_to_field_endpoint_test_splits_multiple_fields() -> None:
    mock_all_responses_content = [
        (
            "magic",
            ["1", "2", "3"],
            RawPseudoMetadata(
                logs=["some-log1"],
                metrics=[{"some-metric1": 1}],
                datadoc=[{"datadoc_metadata": {"variables": [{"magic": "some_arg"}]}}],
            ),
        ),
        (
            "magic",
            ["4", "5", "6"],
            RawPseudoMetadata(
                logs=["some-log2"],
                metrics=[{"some-metric2": 1}],
                datadoc=[{"datadoc_metadata": {"variables": [{"magic": "some_arg"}]}}],
            ),
        ),
        (
            "sorcery",
            ["7", "8", "9"],
            RawPseudoMetadata(
                logs=["sorcery-log"],
                metrics=[{"sorcery-metric": 1}],
                datadoc=[
                    {"datadoc_metadata": {"variables": [{"sorcery": "some_arg"}]}}
                ],
            ),
        ),
    ]
    client = PseudoClient(
        rows_per_partition="1",
        auth_token="some-token",
    )

    mock_pseudo_field_request = Mock(spec=PseudoFieldRequest)
    mock_pseudo_field_request.name = "magic"
    mock_pseudo_field_request.values = ["1", "2", "3", "4", "5", "6"]

    mock_pseudo_field_request = Mock(spec=PseudoFieldRequest)
    mock_pseudo_field_request.name = "sorcery"
    mock_pseudo_field_request.values = ["7", "8", "9"]

    with patch("asyncio.gather", new_callable=AsyncMock) as mock_gather:
        mock_gather.return_value = mock_all_responses_content

        results = await client.post_to_field_endpoint(
            path="test_path",
            pseudo_requests=[mock_pseudo_field_request],
            timeout=TIMEOUT_DEFAULT,
        )

        expected_data = [
            (
                "magic",
                ["1", "2", "3", "4", "5", "6"],
                RawPseudoMetadata(
                    logs=["some-log1", "some-log2"],
                    metrics=[{"some-metric1": 1}, {"some-metric2": 1}],
                    datadoc=[
                        {"datadoc_metadata": {"variables": [{"magic": "some_arg"}]}}
                    ],
                ),
            ),
            (
                "sorcery",
                ["7", "8", "9"],
                RawPseudoMetadata(
                    logs=["sorcery-log"],
                    metrics=[{"sorcery-metric": 1}],
                    datadoc=[
                        {"datadoc_metadata": {"variables": [{"sorcery": "some_arg"}]}}
                    ],
                ),
            ),
        ]

        assert results == expected_data


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
