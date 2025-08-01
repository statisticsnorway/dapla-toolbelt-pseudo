"""Baseclasses for the Pseudo Builders.

The methods are kept private in order to not expose them to users of the client
when using autocomplete-features. The method names should also be more technical
and descriptive than the user-friendly methods that are exposed.
"""

import asyncio
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import date

import polars as pl
from dapla_metadata.datasets.core import Datadoc

from dapla_pseudo.constants import Env
from dapla_pseudo.constants import MapFailureStrategy
from dapla_pseudo.constants import PredefinedKeys
from dapla_pseudo.constants import PseudoFunctionTypes
from dapla_pseudo.constants import PseudoOperation
from dapla_pseudo.utils import build_pseudo_field_request
from dapla_pseudo.utils import convert_to_date
from dapla_pseudo.utils import running_asyncio_loop
from dapla_pseudo.v1.client import PseudoClient
from dapla_pseudo.v1.models.api import DepseudoFieldRequest
from dapla_pseudo.v1.models.api import PseudoFieldRequest
from dapla_pseudo.v1.models.api import PseudoFieldResponse
from dapla_pseudo.v1.models.api import RawPseudoMetadata
from dapla_pseudo.v1.models.api import RepseudoFieldRequest
from dapla_pseudo.v1.models.core import DaeadKeywordArgs
from dapla_pseudo.v1.models.core import FF31KeywordArgs
from dapla_pseudo.v1.models.core import MapSidKeywordArgs
from dapla_pseudo.v1.models.core import PseudoFunction
from dapla_pseudo.v1.models.core import PseudoKeyset
from dapla_pseudo.v1.models.core import PseudoRule
from dapla_pseudo.v1.mutable_dataframe import MutableDataFrame
from dapla_pseudo.v1.result import Result


class _BasePseudonymizer:
    """Base class for the _Pseudonymizer/_Depseudonymizer/_Repseudonymizer builders."""

    def __init__(
        self,
        pseudo_operation: PseudoOperation,
        dataset: pl.DataFrame,
        hierarchical: bool,
        user_provided_metadata: Datadoc | None,
    ) -> None:
        """The constructor of the base class."""
        self._pseudo_operation = pseudo_operation
        self._pseudo_client: PseudoClient = PseudoClient(
            pseudo_service_url=os.getenv(Env.PSEUDO_SERVICE_URL),
            auth_token=os.getenv(Env.PSEUDO_SERVICE_AUTH_TOKEN),
            rows_per_partition=os.getenv(Env.PSEUDO_CLIENT_ROWS_PER_PARTITION),
            max_total_partitions=os.getenv(Env.PSEUDO_CLIENT_MAX_TOTAL_PARTITIONS),
        )
        self._dataset = MutableDataFrame(dataset, hierarchical)
        self._user_provided_metadata = user_provided_metadata

    def _execute_pseudo_operation(
        self,
        rules: list[PseudoRule],  # "source rules" if repseudo
        timeout: int,
        custom_keyset: PseudoKeyset | str | None = None,
        target_custom_keyset: PseudoKeyset | str | None = None,  # used in repseudo
        target_rules: list[PseudoRule] | None = None,  # used in repseudo
    ) -> Result:
        if self._dataset is None:
            raise ValueError("No dataset has been provided.")

        if rules == []:
            raise ValueError(
                "No fields have been provided. Use the 'on_fields' method."
            )

        pseudo_requests = build_pseudo_field_request(
            self._pseudo_operation,
            self._dataset,
            rules,
            custom_keyset,
            target_custom_keyset,
            target_rules,
        )

        pseudo_response = self._pseudonymize_field(pseudo_requests, timeout)
        return Result(
            pseudo_response=pseudo_response,
            pseudo_operation=self._pseudo_operation,
            targeted_columns=[
                pseudo_rule.pattern
                for pseudo_rule in (target_rules if target_rules else rules)
            ],
            user_provided_metadata=self._user_provided_metadata,
        )

    def _pseudonymize_field(
        self,
        pseudo_requests: list[
            PseudoFieldRequest | DepseudoFieldRequest | RepseudoFieldRequest
        ],
        timeout: int,
    ) -> PseudoFieldResponse:
        """Pseudonymizes the specified fields in the DataFrame using the provided pseudonymization function.

        The pseudonymization is performed concurrently. After the processing is finished,
        the pseudonymized fields replace the original fields in the DataFrame stored in `self._dataframe`.
        """
        # type narrowing isn't carried over from caller function
        assert isinstance(self._dataset, MutableDataFrame)
        # Execute the pseudonymization API calls in parallel

        raw_metadata_fields: list[RawPseudoMetadata] = []
        if running_asyncio_loop() is not None:
            with ThreadPoolExecutor(
                1
            ) as pool:  # Run new event loop in a second worker thread if an event loop is already running
                result = pool.submit(
                    lambda: asyncio.run(
                        self._pseudo_client.post_to_field_endpoint(
                            path=f"{self._pseudo_operation.value}/field",
                            timeout=timeout,
                            pseudo_requests=pseudo_requests,
                        )
                    )
                ).result()
        else:
            result = asyncio.run(
                self._pseudo_client.post_to_field_endpoint(
                    path=f"{self._pseudo_operation.value}/field",
                    timeout=timeout,
                    pseudo_requests=pseudo_requests,
                )
            )

        for field_name, data, raw_metadata in result:
            self._dataset.update(field_name, data)
            raw_metadata_fields.append(raw_metadata)

        return PseudoFieldResponse(
            data=self._dataset.to_polars(), raw_metadata=raw_metadata_fields
        )


class _BaseRuleConstructor:
    """Base class for the _PseudoFuncSelector/_DepseudoFuncSelector/_RepseudoFuncSelector builders."""

    def __init__(
        self,
        fields: list[str],
    ) -> None:
        self._fields = fields

    def _map_to_stable_id_and_pseudonymize(
        self,
        sid_snapshot_date: str | date | None = None,
        custom_key: PredefinedKeys | str | None = None,
        on_map_failure: MapFailureStrategy | str | None = None,
    ) -> list[PseudoRule]:
        failure_strategy = (
            None if on_map_failure is None else MapFailureStrategy(on_map_failure)
        )
        kwargs = (
            MapSidKeywordArgs(
                key_id=custom_key,
                snapshot_date=convert_to_date(sid_snapshot_date),
                failure_strategy=failure_strategy,
            )
            if custom_key
            else MapSidKeywordArgs(
                snapshot_date=convert_to_date(sid_snapshot_date),
                failure_strategy=failure_strategy,
            )
        )
        pseudo_func = PseudoFunction(
            function_type=PseudoFunctionTypes.MAP_SID, kwargs=kwargs
        )
        return self._rule_constructor(pseudo_func)

    def _with_daead_encryption(
        self, custom_key: PredefinedKeys | str | None = None
    ) -> list[PseudoRule]:
        kwargs = (
            DaeadKeywordArgs(key_id=custom_key) if custom_key else DaeadKeywordArgs()
        )
        pseudo_func = PseudoFunction(
            function_type=PseudoFunctionTypes.DAEAD, kwargs=kwargs
        )
        return self._rule_constructor(pseudo_func)

    def _with_ff31_encryption(
        self, custom_key: PredefinedKeys | str | None = None
    ) -> list[PseudoRule]:
        kwargs = FF31KeywordArgs(key_id=custom_key) if custom_key else FF31KeywordArgs()
        pseudo_func = PseudoFunction(
            function_type=PseudoFunctionTypes.FF31, kwargs=kwargs
        )
        return self._rule_constructor(pseudo_func)

    def _with_custom_function(self, function: PseudoFunction) -> list[PseudoRule]:
        return self._rule_constructor(function)

    def _rule_constructor(self, func: PseudoFunction) -> list[PseudoRule]:
        # If we use the pseudonymize_file endpoint, we need a glob catch-all prefix.
        rules = [
            PseudoRule(name=None, func=func, pattern=field) for field in self._fields
        ]
        return rules
