from unittest.mock import Mock
from unittest.mock import patch

import pandas as pd
import pytest
import requests

from dapla_pseudo.constants import PredefinedKeys
from dapla_pseudo.constants import PseudoFunctionTypes
from dapla_pseudo.v1.builder import PseudoData
from dapla_pseudo.v1.models import Field
from dapla_pseudo.v1.models import PseudoFunction


PKG = "dapla_pseudo.v1.builder"


@pytest.fixture()
def df() -> pd.DataFrame:
    return pd.DataFrame({"a": ["1", "2", "3"]})


@patch(f"{PKG}._client")
def test_builder_pandas_pseudonymize_minimal_call(patched_client: Mock, df: pd.DataFrame) -> None:
    patched_client.pseudonymize.return_value = requests.Response()
    PseudoData.from_pandas(df).on_field("fornavn").apply_default_encryption()


def test_builder_pandas_pseudo_func_default(df: pd.DataFrame) -> None:
    assert PseudoData.from_pandas(df).on_field("fornavn").apply_default_encryption()._pseudo_func == PseudoFunction(
        function_type=PseudoFunctionTypes.DAEAD, key=PredefinedKeys.SSB_COMMON_KEY_1
    )


def test_builder_pandas_pseudo_func_map_to_sid(df: pd.DataFrame) -> None:
    assert PseudoData.from_pandas(df).on_field(
        "fornavn"
    ).map_to_stable_id_then_apply_fpe()._pseudo_func == PseudoFunction(
        function_type=PseudoFunctionTypes.MAP_SID, key=PredefinedKeys.PAPIS_COMMON_KEY_1
    )


def test_builder_pandas_pseudo_func_multiple_fields(df: pd.DataFrame) -> None:
    fields = ["snr", "snr_mor", "snr_far"]
    assert PseudoData.from_pandas(df).on_fields(*fields)._fields == [Field(pattern=f"**/{f}") for f in fields]


def test_builder_bucket() -> None:
    PseudoData.from_bucket("gs://my-bucket")
