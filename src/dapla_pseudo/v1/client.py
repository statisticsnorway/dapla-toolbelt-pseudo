"""Module that implements a client abstraction that makes it easy to communicate with the Dapla Pseudo Service REST API."""

import asyncio
import os
import typing as t
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
from datetime import date

import google.auth.transport.requests
import google.oauth2.id_token
import msgspec
import requests
from aiohttp import ClientResponse
from aiohttp import ClientSession
from aiohttp import ClientTimeout
from aiohttp import TCPConnector
from aiohttp_retry import ExponentialRetry
from aiohttp_retry import RetryClient
from dapla import AuthClient
from ulid import ULID

from dapla_pseudo.constants import TIMEOUT_DEFAULT
from dapla_pseudo.constants import Env
from dapla_pseudo.constants import PseudoFunctionTypes
from dapla_pseudo.utils import redact_field
from dapla_pseudo.v1.models.api import DepseudoFieldRequest
from dapla_pseudo.v1.models.api import PseudoFieldRequest
from dapla_pseudo.v1.models.api import RawPseudoMetadata
from dapla_pseudo.v1.models.api import RepseudoFieldRequest
from dapla_pseudo.v1.models.core import Mimetypes


class PseudoClient:
    """Client for interacting with the Dapla Pseudo Service REST API."""

    def __init__(
        self,
        pseudo_service_url: str | None = None,
        auth_token: str | None = None,
    ) -> None:
        """Use a default url for dapla-pseudo-service if not explicitly set."""
        self.pseudo_service_url = (
            "http://dapla-pseudo-service.dapla.svc.cluster.local"
            if pseudo_service_url is None
            else pseudo_service_url
        )
        self.static_auth_token = auth_token

    def __auth_token(self) -> str:
        if os.environ.get("DAPLA_REGION") == "CLOUD_RUN":
            audience = os.environ["PSEUDO_SERVICE_URL"]
            auth_req = google.auth.transport.requests.Request()  # type: ignore[no-untyped-call]
            token = t.cast(
                str,
                google.oauth2.id_token.fetch_id_token(auth_req, audience),  # type: ignore[no-untyped-call]
            )
            return token
        else:
            return (
                str(AuthClient.fetch_personal_token())
                if self.static_auth_token is None
                else str(self.static_auth_token)
            )

    async def post_to_field_endpoint(
        self,
        path: str,
        timeout: int,
        pseudo_requests: list[
            PseudoFieldRequest | DepseudoFieldRequest | RepseudoFieldRequest
        ],
    ) -> list[tuple[str, list[str | None], RawPseudoMetadata]]:
        """Post a request to the Pseudo Service field endpoint.

        Args:
            path (str): Full URL to the endpoint
            timeout (int): Request timeout
            pseudo_requests: Pseudo requests

        Returns:
            list[tuple[str, list[str], RawPseudoMetadata]]: A list of tuple of (field_name, data, metadata)
        """

        async def _post(
            client: RetryClient,
            path: str,
            timeout: int,
            request: PseudoFieldRequest | DepseudoFieldRequest | RepseudoFieldRequest,
        ) -> tuple[str, list[str | None], RawPseudoMetadata]:
            if (
                type(request) is PseudoFieldRequest
                and request.pseudo_func.function_type == PseudoFunctionTypes.REDACT
            ):
                return redact_field(request)  # type: ignore[return-value]
            else:
                async with client.post(
                    url=f"{self.pseudo_service_url}/{path}",
                    headers={
                        "Authorization": f"Bearer {self.__auth_token()}",
                        "Content-Type": Mimetypes.JSON.value,
                        "X-Correlation-Id": PseudoClient._generate_new_correlation_id(),
                    },
                    json={"request": request.model_dump(by_alias=True)},
                    timeout=timeout,
                ) as response:
                    await PseudoClient._handle_response_error(response)
                    response_json = await response.json()
                    data: list[str | None] = response_json["data"]
                    metadata = RawPseudoMetadata(
                        field_name=request.name,
                        logs=response_json["logs"],
                        metrics=response_json["metrics"],
                        datadoc=response_json["datadoc_metadata"].get(
                            "variables", None
                        ),
                    )

                    return request.name, data, metadata

        aio_session = ClientSession(
            connector=TCPConnector(limit=200), timeout=ClientTimeout(total=60 * 60 * 24)
        )
        async with RetryClient(
            client_session=aio_session,
            retry_options=ExponentialRetry(
                attempts=5,
                start_timeout=0.1,
                max_timeout=30,
                factor=6,
                statuses={
                    400,
                }.union(
                    set(range(500, 600))
                ),  # Retry all 5xx errors and 400 Bad Request
            ),
        ) as client:
            results = await asyncio.gather(
                *[
                    _post(client=client, path=path, timeout=timeout, request=req)
                    for req in pseudo_requests
                ]
            )

        return results

    def post_to_field_endpoint_sync(
        self,
        path: str,
        timeout: int,
        pseudo_requests: list[
            PseudoFieldRequest | DepseudoFieldRequest | RepseudoFieldRequest
        ],
    ) -> list[tuple[str, list[str | None], RawPseudoMetadata]]:
        """Make requests to the API in a synchronous manner.

        This is needed in case the library is used
        in an environment where a Jupyter environment already exists, e.g. Jupyter Notebook.
        """

        def pseudonymize_field_runner(
            path: str,
            timeout: int,
            request: PseudoFieldRequest | DepseudoFieldRequest | RepseudoFieldRequest,
        ) -> tuple[str, list[str | None], RawPseudoMetadata]:
            if (
                type(request) is PseudoFieldRequest
                and request.pseudo_func.function_type == PseudoFunctionTypes.REDACT
            ):
                return redact_field(request)  # type: ignore[return-value]
            else:
                response = requests.post(
                    url=f"{self.pseudo_service_url}/{path}",
                    headers={
                        "Authorization": f"Bearer {self.__auth_token()}",
                        "Content-Type": Mimetypes.JSON.value,
                        "X-Correlation-Id": PseudoClient._generate_new_correlation_id(),
                    },
                    json={"request": request.model_dump(by_alias=True)},
                    timeout=timeout,
                )
                PseudoClient._handle_response_error_sync(response)
                payload = msgspec.json.decode(response.content.decode("utf-8"))
                data = payload["data"]
                metadata = RawPseudoMetadata(
                    field_name=request.name,
                    logs=payload["logs"],
                    metrics=payload["metrics"],
                    datadoc=payload["datadoc_metadata"].get("variables", None),
                )

                return request.name, data, metadata

        pseudo_results = []
        with ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(pseudonymize_field_runner, path, timeout, r)
                for r in pseudo_requests
            ]
            for future in as_completed(futures):
                pseudo_results.append(future.result())

        return pseudo_results

    @staticmethod
    def _generate_new_correlation_id() -> str:
        return str(ULID())

    @staticmethod
    async def _handle_response_error(response: ClientResponse) -> None:
        """Report error messages in response object."""
        match response.status:
            case status if status in range(200, 300):
                pass
            case _:
                print(response.headers)
                print(await response.text())
                response.raise_for_status()

    @staticmethod
    def _handle_response_error_sync(response: requests.Response) -> None:
        """Report error messages in response object. For synchronous callers."""
        match response.status_code:
            case status if status in range(200, 300):
                pass
            case _:
                print(response.headers)
                print(response.text)
                response.raise_for_status()

    def _post_to_sid_endpoint(
        self,
        path: str,
        values: list[str],
        sid_snapshot_date: date | None = None,
        stream: bool = True,
    ) -> requests.Response:
        request: dict[str, t.Collection[str]] = {"fnrList": values}
        response = requests.post(
            url=f"{self.pseudo_service_url}/{path}",
            params={"snapshot": str(sid_snapshot_date)} if sid_snapshot_date else None,
            # Do not set content-type, as this will cause the json to serialize incorrectly
            headers={
                "Authorization": f"Bearer {self.__auth_token()}",
                "X-Correlation-Id": PseudoClient._generate_new_correlation_id(),
            },
            json=request,
            stream=stream,
            timeout=TIMEOUT_DEFAULT,  # seconds
        )

        PseudoClient._handle_response_error_sync(response)
        return response


def _client() -> PseudoClient:
    return PseudoClient(
        pseudo_service_url=os.getenv(Env.PSEUDO_SERVICE_URL),
        auth_token=os.getenv(Env.PSEUDO_SERVICE_AUTH_TOKEN),
    )
