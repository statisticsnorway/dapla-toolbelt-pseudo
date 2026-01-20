"""Module that implements a client abstraction that makes it easy to communicate with the Dapla Pseudo Service REST API."""

import asyncio
import copy
import os
import typing as t
from collections import defaultdict
from collections.abc import Generator
from datetime import date

import google.auth.transport.requests
import google.oauth2.id_token
import requests
from aiohttp import ClientPayloadError
from aiohttp import ClientResponse
from aiohttp import ClientSession
from aiohttp import ClientTimeout
from aiohttp import ServerDisconnectedError
from aiohttp import TCPConnector
from aiohttp_retry import ExponentialRetry
from aiohttp_retry import RetryClient
from dapla_auth_client import AuthClient
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
        rows_per_partition: str | None = None,
        max_total_partitions: str | None = None,
    ) -> None:
        """Use a default url for dapla-pseudo-service if not explicitly set."""
        self.pseudo_service_url = (
            "http://dapla-pseudo-service.dapla.svc.cluster.local"
            if pseudo_service_url is None
            else pseudo_service_url
        )
        self.static_auth_token = auth_token
        self.rows_per_partition = (
            10000 if rows_per_partition is None else int(rows_per_partition)
        )
        self.max_total_partitions = (
            200 if max_total_partitions is None else int(max_total_partitions)
        )

    def __auth_token(self, current_attempt: int = 0) -> str:
        if os.environ.get("DAPLA_REGION") == "CLOUD_RUN":
            audience = os.environ["PSEUDO_SERVICE_URL"]
            auth_req = google.auth.transport.requests.Request()  # type: ignore[no-untyped-call]

            # Retry logic for fetching token - transiently fails in Cloud Run.
            max_token_fetch_attempts = 3
            try:
                token = t.cast(
                    str,
                    google.oauth2.id_token.fetch_id_token(auth_req, audience),  # type: ignore[no-untyped-call]
                )
            except google.auth.exceptions.DefaultCredentialsError as e:
                if current_attempt < max_token_fetch_attempts - 1:
                    return self.__auth_token(current_attempt + 1)
                else:
                    raise e

            return token
        else:
            return (
                str(AuthClient.fetch_personal_token())
                if self.static_auth_token is None
                else str(self.static_auth_token)
            )

    @staticmethod
    async def is_json_parseable(response: ClientResponse) -> bool:
        """Check if response content is JSON parseable."""
        try:
            await response.json()
            return True
        except Exception:
            return False

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
            path: Full URL to the endpoint
            timeout: Request timeout
            pseudo_requests: Pseudo requests

        Returns:
            list[tuple[str, list[str], RawPseudoMetadata]]: A list of tuple of (field_name, data, metadata)
        """

        async def _post(
            client: RetryClient,
            path: str,
            timeout: int,
            correlation_id: str,
            request: PseudoFieldRequest | DepseudoFieldRequest | RepseudoFieldRequest,
        ) -> tuple[str, list[str | None], RawPseudoMetadata]:
            if (
                type(request) is PseudoFieldRequest
                and request.pseudo_func.function_type == PseudoFunctionTypes.REDACT
            ):
                return redact_field(request)
            else:
                async with client.post(
                    url=f"{self.pseudo_service_url}/{path}",
                    headers={
                        "Authorization": f"Bearer {self.__auth_token()}",
                        "Content-Type": Mimetypes.JSON.value,
                        "X-Correlation-Id": correlation_id,
                    },
                    json={"request": request.model_dump(by_alias=True)},
                    timeout=timeout,
                ) as response:
                    await PseudoClient._handle_response_error(response)
                    response_json = await response.json()
                    data = response_json["data"]
                    metadata = RawPseudoMetadata(
                        field_name=request.name,
                        logs=response_json["logs"],
                        metrics=response_json["metrics"],
                        datadoc=response_json["datadoc_metadata"].get(
                            "variables", None
                        ),
                    )

                    return request.name, data, metadata

        split_pseudo_requests = self._split_requests(pseudo_requests)
        aio_session = ClientSession(
            connector=TCPConnector(limit=100, enable_cleanup_closed=True),
            timeout=ClientTimeout(total=TIMEOUT_DEFAULT),
        )
        async with RetryClient(
            client_session=aio_session,
            retry_options=ExponentialRetry(
                attempts=5,
                start_timeout=0.1,
                max_timeout=30,
                factor=6,
                statuses={400, 401, 429}.union(
                    set(range(500, 600))
                ),  # Retry all 5xx errors and 400 Bad Request
                exceptions={
                    ClientPayloadError,
                    ServerDisconnectedError,
                    asyncio.TimeoutError,
                    OSError,
                },
                evaluate_response_callback=PseudoClient.is_json_parseable,
            ),
        ) as client:
            results = await asyncio.gather(
                *[
                    _post(
                        client=client,
                        path=path,
                        timeout=timeout,
                        request=req,
                        correlation_id=correlation_id,
                    )
                    for (correlation_id, reqs) in split_pseudo_requests.items()
                    for req in reqs
                ]
            )
        await asyncio.sleep(0.5)  # Allow time for sockets to close
        await aio_session.close()

        return PseudoClient._merge_responses(results)

    def _split_requests(
        self,
        field_requests: list[
            PseudoFieldRequest | DepseudoFieldRequest | RepseudoFieldRequest
        ],
    ) -> dict[
        str, list[PseudoFieldRequest | DepseudoFieldRequest | RepseudoFieldRequest]
    ]:
        """Split requests into partitions.

        This is done to limit the size of a single request and more evenly distribute the load across the Pseudo Service.
        """

        def partition_requests(
            field_request: (
                PseudoFieldRequest | DepseudoFieldRequest | RepseudoFieldRequest
            ),
            chunk_size: int,
        ) -> Generator[
            PseudoFieldRequest | DepseudoFieldRequest | RepseudoFieldRequest,
            None,
            None,
        ]:
            for i in range(0, len(field_request.values), chunk_size):
                new_field_request = copy.deepcopy(field_request)
                new_field_request.values = field_request.values[i : i + chunk_size]
                yield new_field_request

        partitioned_field_requests: dict[
            str, list[PseudoFieldRequest | DepseudoFieldRequest | RepseudoFieldRequest]
        ] = defaultdict(list)
        for req in field_requests:
            n_partitions = min(
                self.max_total_partitions // len(field_requests),
                len(req.values) // (self.rows_per_partition * len(field_requests)),
            )
            # We delegate equal amount of partitions to every field.

            if n_partitions <= 1:  # Do not split if number of rows is less than 10k
                partitioned_field_requests[
                    PseudoClient._generate_new_correlation_id()
                ].append(req)
                continue

            correlation_id = PseudoClient._generate_new_correlation_id()
            partition_size = max(1, len(req.values) // n_partitions)
            for request in partition_requests(req, partition_size):
                partitioned_field_requests[correlation_id].append(request)

        return partitioned_field_requests

    @staticmethod
    def _merge_responses(
        responses: list[tuple[str, list[str | None], RawPseudoMetadata]],
    ) -> list[tuple[str, list[str | None], RawPseudoMetadata]]:
        """Merge the response from the Pseudo Service into a single tuple.

        The responses are merged such that the first value of tuple, the field name, is unique.
        The second value, the pseudonymized data, gets concatenated with the other field names.
        The third value, the metadata - for Datadoc metadata they should always be equal, so we just keep the first one encountered.
        For logs and metrics, we concatenate the lists/dictionaries of lists.
        """
        grouped: dict[str, tuple[list[str | None], RawPseudoMetadata]] = defaultdict(
            lambda: ([], RawPseudoMetadata(logs=[], metrics=[], datadoc=None))
        )

        for key, string_list, metadata in responses:
            grouped[key] = (grouped[key][0] + (string_list), grouped[key][1] + metadata)

        return [(k, v[0], v[1]) for k, v in grouped.items()]

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

    async def _post_to_sid_endpoint(
        self,
        path: str,
        values: list[str],
        sid_snapshot_date: date | None = None,
    ) -> tuple[list[str], str | None]:
        """Post SID lookup in batches concurrently and merge responses.

        Returns:
            tuple[list[str], str | None]: (missing_values, datasetExtractionSnapshotTime)
        """
        total_rows = len(values)
        batch_size = self.rows_per_partition

        # Do not split if total rows is less than batch size (default 10k)
        if total_rows <= batch_size:
            batches = [values]
        else:
            batches = [
                values[i : i + batch_size] for i in range(0, total_rows, batch_size)
            ]

        async with ClientSession(
            connector=TCPConnector(limit=100, enable_cleanup_closed=True),
            timeout=ClientTimeout(total=TIMEOUT_DEFAULT),
        ) as session:

            async def _post_batch(
                batch: list[str],
                path: str,
            ) -> dict[str, list[str] | str]:
                resp_cm = await session.post(
                    url=f"{self.pseudo_service_url}/{path}",
                    params=(
                        {"snapshot": str(sid_snapshot_date)}
                        if sid_snapshot_date
                        else None
                    ),
                    # Do not set content-type, as this will cause the json to serialize incorrectly
                    headers={
                        "Authorization": f"Bearer {self.__auth_token()}",
                        "X-Correlation-Id": PseudoClient._generate_new_correlation_id(),
                    },
                    json={"fnrList": batch},
                )
                async with resp_cm as response:
                    await PseudoClient._handle_response_error(response)
                    payload = await response.json()
                    return t.cast(
                        dict[str, list[str] | str],
                        (
                            payload[0]
                            if isinstance(payload, list) and payload
                            else payload
                        ),
                    )

            results = await asyncio.gather(*[_post_batch(b, path) for b in batches])

        # Merge results
        all_missing = [m for r in results for m in r.get("missing", [])]
        raw_snapshot = (
            results[0].get("datasetExtractionSnapshotTime") if results else None
        )
        snapshot_time = t.cast(str | None, raw_snapshot)
        return all_missing, snapshot_time


def _client() -> PseudoClient:
    return PseudoClient(
        pseudo_service_url=os.getenv(Env.PSEUDO_SERVICE_URL),
        auth_token=os.getenv(Env.PSEUDO_SERVICE_AUTH_TOKEN),
    )
