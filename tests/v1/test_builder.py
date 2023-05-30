import json
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
    with open("tests/data/personer.json") as test_data:
        return pd.json_normalize(json.load(test_data))


@patch(f"{PKG}._client")
def test_builder_pandas_pseudonymize_minimal_call(patched_client: Mock, df: pd.DataFrame) -> None:
    patched_client.pseudonymize.return_value = requests.Response()
    PseudoData.from_pandas(df).on_field("fornavn").apply_default_encryption()


def test_builder_fields_selector_single_field(df: pd.DataFrame) -> None:
    PseudoData.from_pandas(df).on_field("fornavn")._fields = [Field(pattern="**/fornavn")]


def test_builder_fields_selector_multiple_fields(df: pd.DataFrame) -> None:
    PseudoData.from_pandas(df).on_fields("fornavn", "fnr")._fields = [
        Field(pattern="**/fornavn"),
        Field(pattern="**/fnr"),
    ]


def test_builder_pseudo_function_selector_default(df: pd.DataFrame) -> None:
    assert PseudoData.from_pandas(df).on_field("fornavn").apply_default_encryption()._pseudo_func == PseudoFunction(
        function_type=PseudoFunctionTypes.DAEAD, key=PredefinedKeys.SSB_COMMON_KEY_1
    )


def test_builder_pseudo_function_selector_map_to_sid(df: pd.DataFrame) -> None:
    assert PseudoData.from_pandas(df).on_field("fnr").map_to_stable_id_then_apply_fpe()._pseudo_func == PseudoFunction(
        function_type=PseudoFunctionTypes.MAP_SID, key=PredefinedKeys.PAPIS_COMMON_KEY_1
    )


def test_builder_pseudo_function_selector_fpe(df: pd.DataFrame) -> None:
    assert PseudoData.from_pandas(df).on_field("fnr").apply_fpe()._pseudo_func == PseudoFunction(
        function_type=PseudoFunctionTypes.FF31, key=PredefinedKeys.PAPIS_COMMON_KEY_1, extra_kwargs=["strategy=SKIP"]
    )


def test_builder_pseudo_function_selector_custom(df: pd.DataFrame) -> None:
    assert PseudoData.from_pandas(df).on_field("fnr").apply_custom_pseudo_function(
        PseudoFunctionTypes.FF31, PredefinedKeys.SSB_COMMON_KEY_2
    )._pseudo_func == PseudoFunction(function_type=PseudoFunctionTypes.FF31, key=PredefinedKeys.SSB_COMMON_KEY_2)


def test_builder_pandas_pseudo_func_multiple_fields(df: pd.DataFrame) -> None:
    fields = ["snr", "snr_mor", "snr_far"]
    assert PseudoData.from_pandas(df).on_fields(*fields)._fields == [Field(pattern=f"**/{f}") for f in fields]


def test_builder_bucket() -> None:
    PseudoData.from_bucket("gs://my-bucket/my-data.csv")