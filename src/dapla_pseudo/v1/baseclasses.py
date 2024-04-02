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
from typing import Optional

import polars as pl
import requests

from dapla_pseudo.constants import Env
from dapla_pseudo.constants import PredefinedKeys
from dapla_pseudo.constants import PseudoFunctionTypes
from dapla_pseudo.constants import PseudoOperation
from dapla_pseudo.types import FileSpecDecl
from dapla_pseudo.utils import convert_to_date
from dapla_pseudo.v1.client import PseudoClient
from dapla_pseudo.v1.client import _extract_name
from dapla_pseudo.v1.models.api import DaeadKeywordArgs
from dapla_pseudo.v1.models.api import FF31KeywordArgs
from dapla_pseudo.v1.models.api import KeyWrapper
from dapla_pseudo.v1.models.api import MapSidKeywordArgs
from dapla_pseudo.v1.models.api import Mimetypes
from dapla_pseudo.v1.models.api import PseudoConfig
from dapla_pseudo.v1.models.api import PseudoFieldRequest
from dapla_pseudo.v1.models.api import PseudoFunction
from dapla_pseudo.v1.models.api import PseudoKeyset
from dapla_pseudo.v1.models.api import PseudonymizeFileRequest
from dapla_pseudo.v1.models.api import PseudoRule
from dapla_pseudo.v1.models.core import File
from dapla_pseudo.v1.models.core import PseudoFieldResponse
from dapla_pseudo.v1.models.core import PseudoFileResponse
from dapla_pseudo.v1.models.core import RawPseudoMetadata
from dapla_pseudo.v1.result import Result


class _BasePseudonymizer:
    def __init__(self, pseudo_operation: PseudoOperation) -> None:
        self._pseudo_operation = pseudo_operation
        self._pseudo_client: PseudoClient = PseudoClient(
            pseudo_service_url=os.getenv(Env.PSEUDO_SERVICE_URL),
            auth_token=os.getenv(Env.PSEUDO_SERVICE_AUTH_TOKEN),
        )

    def _execute_pseudo_operation(
        self,
        dataset: File | pl.DataFrame,
        rules: list[PseudoRule],
        timeout: int,
        custom_keyset: Optional[PseudoKeyset | str] = None,
    ) -> Result:
        if dataset is None:
            raise ValueError("No dataset has been provided.")

        if rules == []:
            raise ValueError(
                "No fields have been provided. Use the 'on_fields' method."
            )

        match dataset:  # Differentiate between file and DataFrame
            case File():
                return self._pseudonymize_file(dataset, rules, timeout, custom_keyset)
            case pl.DataFrame():
                return self._pseudonymize_field(dataset, rules, timeout, custom_keyset)
            case _ as invalid_dataset:
                raise ValueError(
                    f"Unsupported data type: {type(invalid_dataset)}. Should only be DataFrame or file-like type."
                )

    def _pseudonymize_field(
        self,
        dataframe: pl.DataFrame,
        rules: list[PseudoRule],
        timeout: int,
        custom_keyset: Optional[PseudoKeyset | str] = None,
    ) -> Result:
        """Pseudonymizes the specified fields in the DataFrame using the provided pseudonymization function.

        The pseudonymization is performed in parallel. After the parallel processing is finished,
        the pseudonymized fields replace the original fields in the DataFrame stored in `self._dataframe`.
        """

        def pseudonymize_field_runner(
            field_name: str, series: pl.Series, pseudo_func: PseudoFunction
        ) -> tuple[str, pl.Series, RawPseudoMetadata]:
            """Function that performs the pseudonymization on a Polars Series."""
            request = PseudoFieldRequest(
                pseudo_func=pseudo_func,
                keyset=KeyWrapper(custom_keyset).keyset,
                name=field_name,
                values=series.to_list(),
            )

            response: requests.Response = self._pseudo_client._post_to_field_endpoint(
                f"{self._pseudo_operation.value}/field",
                request,
                timeout,
                stream=True,
            )
            payload = json.loads(response.content.decode("utf-8"))
            data = payload["data"]
            metadata = RawPseudoMetadata(
                field_name=field_name,
                logs=payload["logs"],
                metrics=payload["metrics"],
                datadoc=payload["datadoc_metadata"]["pseudo_variables"],
            )

            return field_name, pl.Series(data), metadata

        # Execute the pseudonymization API calls in parallel
        with ThreadPoolExecutor() as executor:
            pseudonymized_field: dict[str, pl.Series] = {}
            raw_metadata_fields: list[RawPseudoMetadata] = []
            futures = [
                executor.submit(
                    pseudonymize_field_runner,
                    rule.pattern,
                    dataframe[rule.pattern],
                    rule.func,
                )
                for rule in rules
            ]
            # Wait for the futures to finish, then add each field to pseudonymized_field map
            for future in as_completed(futures):
                field_name, data, raw_metadata = future.result()
                pseudonymized_field[field_name] = data
                raw_metadata_fields.append(raw_metadata)

            pseudonymized_df = pl.DataFrame(pseudonymized_field)
            dataframe = dataframe.update(pseudonymized_df)
        return Result(
            pseudo_response=PseudoFieldResponse(
                data=dataframe, raw_metadata=raw_metadata_fields
            )
        )

    def _pseudonymize_file(
        self,
        file: File,
        rules: list[PseudoRule],
        timeout: int,
        custom_keyset: Optional[PseudoKeyset | str] = None,
    ) -> Result:

        pseudonymize_request = PseudonymizeFileRequest(
            pseudo_config=PseudoConfig(
                rules=rules,
                keysets=KeyWrapper(custom_keyset).keyset_list(),
            ),
            target_content_type=Mimetypes.JSON,
            target_uri=None,
            compression=None,
        )

        request_spec: FileSpecDecl = (
            None,
            pseudonymize_request.to_json(),
            str(Mimetypes.JSON),
        )

        file_name = _extract_name(
            file_handle=file.file_handle, input_content_type=file.content_type
        )

        data_spec: FileSpecDecl = (
            file_name,
            file.file_handle,
            str(pseudonymize_request.target_content_type),
        )

        response = self._pseudo_client._post_to_file_endpoint(
            path=f"{self._pseudo_operation.value}/file",
            request_spec=request_spec,
            data_spec=data_spec,
            stream=True,
            timeout=timeout,
        )

        file.file_handle.close()

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
    def __init__(
        self, fields: list[str], dataset_type: type[pl.DataFrame] | type[File]
    ) -> None:
        self._fields = fields
        self._dataset_type = dataset_type

    def _map_to_stable_id_and_pseudonymize(
        self,
        sid_snapshot_date: Optional[str | date] = None,
        custom_key: Optional[PredefinedKeys | str] = None,
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
        self, custom_key: Optional[PredefinedKeys | str] = None
    ) -> list[PseudoRule]:
        kwargs = (
            DaeadKeywordArgs(key_id=custom_key) if custom_key else DaeadKeywordArgs()
        )
        pseudo_func = PseudoFunction(
            function_type=PseudoFunctionTypes.DAEAD, kwargs=kwargs
        )
        return self._rule_constructor(pseudo_func)

    def _with_ff31_encryption(
        self, custom_key: Optional[PredefinedKeys | str] = None
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
