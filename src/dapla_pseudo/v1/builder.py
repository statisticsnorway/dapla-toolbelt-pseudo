"""Builder for submitting a pseudonymization request."""

from dataclasses import dataclass
from typing import Optional

import pandas as pd
import requests
from typing_extensions import Self

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
    """Starting point for pseudonymization of datasets.

    This class should not be instantiated, only the static methods should be used.
    """

    @staticmethod
    def from_pandas(dataframe: pd.DataFrame) -> "_FieldSelector":
        """Initialize a pseudonymization request from a pandas DataFrame."""
        return PseudoData._FieldSelector(dataframe)

    class _FieldSelector:
        """Select one or multiple fields to be pseudonymized."""

        def __init__(self, dataframe: pd.DataFrame):
            """Initialize the class."""
            self._dataframe = dataframe

        def on_field(self, field: str) -> "PseudoData._PseudoFunctionSelector":
            """Specify a single field to be pseudonymized."""
            return PseudoData._Pseudonymizer(self._dataframe, [Field(pattern=f"**/{field}")])

        def on_fields(self, *fields: str) -> "PseudoData._PseudoFunctionSelector":
            """Specify multiple fields to be pseudonymized."""
            return PseudoData._Pseudonymizer(self._dataframe, [Field(pattern=f"**/{f}") for f in fields])

    class _Pseudonymizer:
        def __init__(self, dataframe: pd.DataFrame, fields: list[Field]) -> None:
            self._dataframe: pd.DataFrame = dataframe
            self._fields: list[Field] = fields
            self._pseudo_func: Optional[PseudoFunction] = None

        def map_to_stable_id(self) -> Self:
            self._pseudo_func = PseudoFunction(
                function_type=PseudoFunctionTypes.MAP_SID, key=PredefinedKeys.PAPIS_COMMON_KEY_1
            )
            return self

        def pseudonymize(
            self, preserve_formatting: bool = False, with_custom_function: PseudoFunction = None
        ) -> "PseudonymizationResult":
            # If _pseudo_func has been defined upstream, then use that.
            if self._pseudo_func is None:
                # If the user has explicitly defined their own function, then use that.
                if with_custom_function is not None:
                    self._pseudo_func = with_custom_function

                # Use Format Preserving Encryption with the PAPIS compatible key (non-default case).
                elif preserve_formatting:
                    self._pseudo_func = PseudoFunction(
                        function_type=PseudoFunctionTypes.FF31,
                        key=PredefinedKeys.PAPIS_COMMON_KEY_1,
                        extra_kwargs=["strategy=SKIP"],
                    )
                # Use DAEAD with the SSB common key as a sane default.
                else:
                    self._pseudo_func = PseudoFunction(
                        function_type=PseudoFunctionTypes.DAEAD, key=PredefinedKeys.SSB_COMMON_KEY_1
                    )

            return _do_pseudonymization(dataframe=self._dataframe, fields=self._fields, pseudo_func=self._pseudo_func)


def _do_pseudonymization(dataframe: pd.DataFrame, fields: list[Field], pseudo_func: PseudoFunction):
    pseudonymize_request = PseudonymizeFileRequest(
        pseudo_config=PseudoConfig(
            rules=[
                PseudoRule(
                    name=f"{f.pattern.split('/')[-1]}-{pseudo_func}",
                    pattern=f.pattern,
                    func=str(pseudo_func),
                )
                for f in fields
            ],
            keysets=KeyWrapper(pseudo_func.key).keyset_list(),
        ),
        target_content_type=Mimetypes.JSON,
        target_uri=None,
        compression=None,
    )
    response: requests.Response = _client().pseudonymize(
        pseudonymize_request, _dataframe_to_json(dataframe), stream=True
    )
    return PseudonymizationResult(dataframe=pd.json_normalize(response.json()))
