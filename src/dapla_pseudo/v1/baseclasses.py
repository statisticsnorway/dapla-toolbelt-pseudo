"""Baseclasses for the Pseudo Builders.

The methods are kept private in order to not expose them to users of the client
when using autocomplete-features. The method names should also be more technical
and descriptive than the user-friendly methods that are exposed.
"""

import json
import os
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
from datetime import date

import polars as pl
import requests

from dapla_pseudo.constants import Env
from dapla_pseudo.constants import PredefinedKeys
from dapla_pseudo.constants import PseudoFunctionTypes
from dapla_pseudo.constants import PseudoOperation
from dapla_pseudo.types import FileSpecDecl
from dapla_pseudo.utils import build_pseudo_dataset_request
from dapla_pseudo.utils import build_pseudo_field_request
from dapla_pseudo.utils import convert_to_date
from dapla_pseudo.v1.client import PseudoClient
from dapla_pseudo.v1.client import _extract_name
from dapla_pseudo.v1.models.api import DepseudoFieldRequest
from dapla_pseudo.v1.models.api import DepseudoFileRequest
from dapla_pseudo.v1.models.api import PseudoFieldRequest
from dapla_pseudo.v1.models.api import PseudoFieldResponse
from dapla_pseudo.v1.models.api import PseudoFileRequest
from dapla_pseudo.v1.models.api import PseudoFileResponse
from dapla_pseudo.v1.models.api import RawPseudoMetadata
from dapla_pseudo.v1.models.api import RepseudoFieldRequest
from dapla_pseudo.v1.models.api import RepseudoFileRequest
from dapla_pseudo.v1.models.core import DaeadKeywordArgs
from dapla_pseudo.v1.models.core import FF31KeywordArgs
from dapla_pseudo.v1.models.core import File
from dapla_pseudo.v1.models.core import HierarchicalDataFrame
from dapla_pseudo.v1.models.core import MapSidKeywordArgs
from dapla_pseudo.v1.models.core import Mimetypes
from dapla_pseudo.v1.models.core import PseudoFunction
from dapla_pseudo.v1.models.core import PseudoKeyset
from dapla_pseudo.v1.models.core import PseudoRule
from dapla_pseudo.v1.result import Result


class _BasePseudonymizer:
    """Base class for the _Pseudonymizer/_Depseudonymizer/_Repseudonymizer builders.

    The constructor of this class takes parameters that are NOT changed during
    the course of a 'Pseudonymizer' builder, i.e. through the 'on_fields()' method.

    The remainder of the parameters are passed to
    :meth:`~baseclasses._BasePseudonymizer._execute_pseudo_operation`.
    """

    def __init__(
        self,
        pseudo_operation: PseudoOperation,
        dataset: File | pl.DataFrame | HierarchicalDataFrame,
    ) -> None:
        """The constructor of the base class."""
        self._pseudo_operation = pseudo_operation
        self._pseudo_client: PseudoClient = PseudoClient(
            pseudo_service_url=os.getenv(Env.PSEUDO_SERVICE_URL),
            auth_token=os.getenv(Env.PSEUDO_SERVICE_AUTH_TOKEN),
        )
        self._dataset = dataset

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

        match self._dataset:  # Differentiate between file and DataFrame
            case pl.DataFrame():
                pseudo_requests = build_pseudo_field_request(
                    self._pseudo_operation,
                    self._dataset,
                    rules,
                    custom_keyset,
                    target_custom_keyset,
                    target_rules,
                )
                return self._pseudonymize_field(pseudo_requests, timeout)
            case File():
                pseudo_request = build_pseudo_dataset_request(
                    self._pseudo_operation,
                    rules,
                    custom_keyset,
                    target_custom_keyset,
                    target_rules,
                )
                return self._pseudonymize_dataset(
                    self._dataset, pseudo_request, timeout
                )
            case HierarchicalDataFrame():
                pseudo_request = build_pseudo_dataset_request(
                    self._pseudo_operation,
                    rules,
                    custom_keyset,
                    target_custom_keyset,
                    target_rules,
                )
                return self._pseudonymize_dataset(
                    self._dataset.contents, pseudo_request, timeout
                )

            case _ as invalid_dataset:
                raise ValueError(
                    f"Unsupported data type: {type(invalid_dataset)}. Should only be DataFrame or file-like type."
                )

    def _pseudonymize_field(
        self,
        pseudo_requests: list[
            PseudoFieldRequest | DepseudoFieldRequest | RepseudoFieldRequest
        ],
        timeout: int,
    ) -> Result:
        """Pseudonymizes the specified fields in the DataFrame using the provided pseudonymization function.

        The pseudonymization is performed in parallel. After the parallel processing is finished,
        the pseudonymized fields replace the original fields in the DataFrame stored in `self._dataframe`.
        """

        def pseudonymize_field_runner(
            request: PseudoFieldRequest | DepseudoFieldRequest | RepseudoFieldRequest,
        ) -> tuple[str, pl.Series, RawPseudoMetadata]:
            """Function that performs the pseudonymization on a Polars Series."""
            response: requests.Response = self._pseudo_client._post_to_field_endpoint(
                f"{self._pseudo_operation.value}/field",
                request,
                timeout,
                stream=True,
            )
            payload = json.loads(response.content.decode("utf-8"))
            data = payload["data"]
            metadata = RawPseudoMetadata(
                field_name=request.name,
                logs=payload["logs"],
                metrics=payload["metrics"],
                datadoc=payload["datadoc_metadata"]["pseudo_variables"],
            )

            return request.name, pl.Series(data), metadata

        # type narrowing isn't carried over from previous function
        assert isinstance(self._dataset, pl.DataFrame)
        # Execute the pseudonymization API calls in parallel
        with ThreadPoolExecutor() as executor:
            raw_metadata_fields: list[RawPseudoMetadata] = []
            futures = [
                executor.submit(pseudonymize_field_runner, request)
                for request in pseudo_requests
            ]
            # Wait for the futures to finish, then add each field to pseudonymized_field map
            for future in as_completed(futures):
                field_name, data, raw_metadata = future.result()
                self._dataset = self._dataset.with_columns(data.alias(field_name))
                raw_metadata_fields.append(raw_metadata)

        return Result(
            pseudo_response=PseudoFieldResponse(
                data=self._dataset, raw_metadata=raw_metadata_fields
            )
        )

    def _pseudonymize_dataset(
        self,
        dataset: File | pl.DataFrame,
        pseudo_request: PseudoFileRequest | DepseudoFileRequest | RepseudoFileRequest,
        timeout: int,
    ) -> Result:
        data_spec: FileSpecDecl

        request_spec: FileSpecDecl = (
            None,
            pseudo_request.to_json(),
            str(Mimetypes.JSON),
        )

        match dataset:
            case File(file_handle, content_type):
                file_name = _extract_name(
                    file_handle=file_handle, input_content_type=content_type
                )
                data_spec = (
                    file_name,
                    file_handle,
                    str(pseudo_request.target_content_type),
                )

                response = self._pseudo_client._post_to_file_endpoint(
                    path=f"{self._pseudo_operation.value}/file",
                    request_spec=request_spec,
                    data_spec=data_spec,
                    stream=True,
                    timeout=timeout,
                )
                file_handle.close()
            case pl.DataFrame():
                file_name = "data.json"
                data_spec = (
                    file_name,
                    json.dumps(dataset.to_dicts()),
                    str(pseudo_request.target_content_type),
                )

                response = self._pseudo_client._post_to_file_endpoint(
                    path=f"{self._pseudo_operation.value}/file",
                    request_spec=request_spec,
                    data_spec=data_spec,
                    stream=True,
                    timeout=timeout,
                )

        payload = json.loads(response.content.decode("utf-8"))
        pseudo_data = payload["data"]
        metadata = RawPseudoMetadata(
            logs=payload["logs"],
            metrics=payload["metrics"],
            datadoc=payload["datadoc_metadata"]["pseudo_variables"],
        )

        return Result(
            PseudoFileResponse(
                data=pseudo_data,
                raw_metadata=metadata,
                content_type=Mimetypes.JSON,
                streamed=True,
                file_name=file_name,
            )
        )


class _BaseRuleConstructor:
    """Base class for the _PseudoFuncSelector/_DepseudoFuncSelector/_RepseudoFuncSelector builders."""

    def __init__(
        self,
        fields: list[str],
        dataset_type: type[pl.DataFrame] | type[File] | type[HierarchicalDataFrame],
    ) -> None:
        self._fields = fields
        self._dataset_type = dataset_type

    def _map_to_stable_id_and_pseudonymize(
        self,
        sid_snapshot_date: str | date | None = None,
        custom_key: PredefinedKeys | str | None = None,
    ) -> list[PseudoRule]:
        kwargs = (
            MapSidKeywordArgs(
                key_id=custom_key,
                snapshot_date=convert_to_date(sid_snapshot_date),
            )
            if custom_key
            else MapSidKeywordArgs(snapshot_date=convert_to_date(sid_snapshot_date))
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
        rule_prefix = "**/" if self._dataset_type == File else ""
        rules = [
            PseudoRule(name=None, func=func, pattern=f"{rule_prefix}{field}")
            for field in self._fields
        ]
        return rules
