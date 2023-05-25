"""Builder for submitting a pseudonymization request."""

from dataclasses import dataclass

import pandas as pd
import requests

from dapla_pseudo.constants import predefined_keys
from dapla_pseudo.constants import pseudo_function_types
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


class Dataset:
    """Starting point for pseudonymizing a single field.

    This class should not be instantiated, only the static methods should be used.
    """

    @staticmethod
    def from_pandas(dataframe: pd.DataFrame) -> "_FieldSelector":
        """Initialize a pseudonymization request from a pandas DataFrame"""
        return Dataset._FieldSelector(dataframe)

    class _FieldSelector:
        def __init__(self, dataframe: pd.DataFrame):
            """Initialize the class."""
            self._dataframe = dataframe

        def on_field(self, field: str) -> "Dataset._Pseudonymizer":
            """Specify the field to be pseudonymized"""
            # Extract/specify field
            return Dataset._Pseudonymizer(self._dataframe, Field(pattern=f"**/{field}"))

    class _Pseudonymizer:
        def __init__(self, dataframe: pd.DataFrame, field: Field) -> None:
            self._dataframe: pd.DataFrame = dataframe
            self._field: Field = field
            self._pseudo_func: PseudoFunction = PseudoFunction(
                function_type=pseudo_function_types.DAEAD, key=predefined_keys.SSB_COMMON_KEY_1
            )

        def map_to_sid(self) -> "Dataset._Pseudonymizer":
            """Specify use of the 'map-sid' pseudo function.

            This should only be used for Norwegian personal numbers for which we wish
            to first map the personal number to an SSB "Stable identifier" (sid)
            and subsequently pseudonymize the sid using Format Preserving Encryption.
            """
            self._pseudo_func = PseudoFunction(
                function_type=pseudo_function_types.MAP_SID, key=predefined_keys.PAPIS_COMMON_KEY_1
            )
            return self

        def pseudonymize(self) -> "PseudonymizationResult":
            pseudonymize_request = PseudonymizeFileRequest(
                pseudo_config=PseudoConfig(
                    rules=[PseudoRule(pattern=self._field.pattern, func=str(self._pseudo_func))],
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
