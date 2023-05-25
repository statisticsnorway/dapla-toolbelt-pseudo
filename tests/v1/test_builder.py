from unittest.mock import Mock
from unittest.mock import patch

import pandas as pd
import pytest
import requests

from dapla_pseudo.v1.builder import Dataset


PKG = "dapla_pseudo.v1.builder"


@pytest.fixture()
def df() -> pd.DataFrame:
    return pd.DataFrame()


@patch(f"{PKG}._client")
def test_pandas_pseudonymize_minimal_call(patched_client: Mock, df: pd.DataFrame):
    patched_client.pseudonymize.return_value = requests.Response()
    Dataset.from_pandas(df).on_field("").pseudonymize()
