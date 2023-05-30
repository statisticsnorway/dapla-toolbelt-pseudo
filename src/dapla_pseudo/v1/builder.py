"""Builder for submitting a pseudonymization request."""

from dataclasses import dataclass
from typing import Optional

import pandas as pd
import requests

from dapla_pseudo.constants import PredefinedKeys
from dapla_pseudo.constants import PseudoFunctionTypes
from dapla_pseudo.v1.models import Field
from dapla_pseudo.v1.models import KeyWrapper
from dapla_pseudo.v1.models import Mimetypes
from dapla_pseudo.v1.models import PseudoConfig
from dapla_pseudo.v1.models import PseudoFunction
from dapla_pseudo.v1.models import PseudonymizeFileRequest
from dapla_pseudo.v1.models import PseudoRule
from dapla_pseudo.v1.ops import _client
from dapla_pseudo.v1.ops import _dataframe_to_json


@dataclass
class PseudonymizationResult:
    """Holder for data and metadata returned from pseudo-service"""

    dataframe: pd.DataFrame


class PseudoData:
    """Starting point for pseudonymizing a single field.

    This class should not be instantiated, only the static methods should be used.
    """

    @staticmethod
    def from_pandas(dataframe: pd.DataFrame) -> "_FieldSelector":
        """Initialize a pseudonymization request from a pandas DataFrame."""
        return PseudoData._FieldSelector(dataframe)

    @staticmethod
    def from_bucket(bucket_uri: str) -> "_FieldSelector":
        """Initialize a pseudonymization request from a csv or json file in a bucket."""
        raise NotImplementedError()

    class _FieldSelector:
        """Select one or multiple fields to be pseudonymized."""

        def __init__(self, dataframe: pd.DataFrame):
            """Initialize the class."""
            self._dataframe = dataframe

        def on_field(self, field: str) -> "PseudoData._PseudoFunctionSelector":
            """Specify a single field to be pseudonymized."""
            return PseudoData._PseudoFunctionSelector(self._dataframe, [Field(pattern=f"**/{field}")])

        def on_fields(self, *fields: str) -> "PseudoData._PseudoFunctionSelector":
            """Specify multiple fields to be pseudonymized."""
            return PseudoData._PseudoFunctionSelector(self._dataframe, [Field(pattern=f"**/{f}") for f in fields])

    class _PseudoFunctionSelector:
        def __init__(self, dataframe: pd.DataFrame, fields: list[Field]) -> None:
            self._dataframe: pd.DataFrame = dataframe
            self._fields: list[Field] = fields

        def apply_default_encryption(self) -> "PseudoData._Pseudonymizer":
            """Pseudonymize using the 'daead' pseudo function."""
            pseudo_func = PseudoFunction(function_type=PseudoFunctionTypes.DAEAD, key=PredefinedKeys.SSB_COMMON_KEY_1)
            return PseudoData._Pseudonymizer(self._dataframe, self._fields, pseudo_func)

        def map_to_stable_id_then_apply_fpe(self) -> "PseudoData._Pseudonymizer":
            """Pseudonymize using the 'map-sid' pseudo function.

            This should only be used for Norwegian personal numbers for which we wish
            to first map the personal number to an SSB "Stable identifier" (aka snr, sid)
            and subsequently pseudonymize the sid using Format Preserving Encryption.
            """
            pseudo_func = PseudoFunction(
                function_type=PseudoFunctionTypes.MAP_SID, key=PredefinedKeys.PAPIS_COMMON_KEY_1
            )
            return PseudoData._Pseudonymizer(self._dataframe, self._fields, pseudo_func)

        def apply_fpe(self) -> "PseudoData._Pseudonymizer":
            """Pseudonymize using the 'ff31' pseudo function.

            This should be used for Stable IDs (snr) which must be compatible with pseudonyms created on-prem.
            """
            pseudo_func = PseudoFunction(
                function_type=PseudoFunctionTypes.FF31,
                key=PredefinedKeys.PAPIS_COMMON_KEY_1,
                extra_kwargs=["strategy=SKIP"],
            )
            return PseudoData._Pseudonymizer(self._dataframe, self._fields, pseudo_func)

        def apply_custom_pseudo_function(
            self,
            function_type: str | PseudoFunctionTypes,
            key: str | PredefinedKeys,
            extra_kwargs: Optional[list[str]] = None,
        ) -> "PseudoData._Pseudonymizer":
            """Pseudonymize using the specified pseudo function."""
            pseudo_func = PseudoFunction(function_type=function_type, key=key, extra_kwargs=extra_kwargs)
            return PseudoData._Pseudonymizer(self._dataframe, self._fields, pseudo_func)

    class _Pseudonymizer:
        def __init__(self, dataframe: pd.DataFrame, fields: list[Field], pseudo_func: PseudoFunction) -> None:
            self._dataframe: pd.DataFrame = dataframe
            self._fields: list[Field] = fields
            self._pseudo_func: PseudoFunction = pseudo_func

        def pseudonymize(self) -> "PseudonymizationResult":
            pseudonymize_request = PseudonymizeFileRequest(
                pseudo_config=PseudoConfig(
                    rules=[
                        PseudoRule(
                            name=f"{f.pattern.split('/')[-1]}-{self._pseudo_func}",
                            pattern=f.pattern,
                            func=str(self._pseudo_func),
                        )
                        for f in self._fields
                    ],
                    keysets=KeyWrapper(self._pseudo_func.key).keyset_list(),
                ),
                target_content_type=Mimetypes.JSON,
                target_uri=None,
                compression=None,
            )
            response: requests.Response = _client().pseudonymize(
                pseudonymize_request, _dataframe_to_json(self._dataframe), stream=True
            )
            return PseudonymizationResult(dataframe=pd.json_normalize(response.json()))
