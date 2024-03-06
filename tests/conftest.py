from unittest.mock import MagicMock

import pandas as pd
import polars as pl
import pytest


@pytest.fixture
def df_personer() -> pl.DataFrame:
    JSON_FILE = "tests/data/personer.json"
    return pl.read_json(
        JSON_FILE,
        schema={
            "fnr": pl.String,
            "fornavn": pl.String,
            "etternavn": pl.String,
            "kjonn": pl.String,
            "fodselsdato": pl.String,
        },
    )


@pytest.fixture
def df_personer_pandas() -> pd.DataFrame:
    JSON_FILE = "tests/data/personer.json"
    return pd.read_json(
        JSON_FILE,
        dtype={
            "fnr": str,
            "fornavn": str,
            "etternavn": str,
            "kjonn": str,
            "fodselsdato": str,
        },
    )


@pytest.fixture()
def personer_hierarch_file_path() -> str:
    return "tests/data/personer_hierarchical.json"


@pytest.fixture()
def personer_pseudonymized_hierarch_file_path() -> str:
    return "tests/data/personer_hierarchical_pseudonymized.json"


@pytest.fixture
def personer_file_path() -> str:
    return "tests/data/personer.json"


@pytest.fixture()
def personer_pseudonymized_file_path() -> str:
    return "tests/data/personer_pseudonymized_default_encryption.json"


@pytest.fixture
def df_personer_fnr_daead_encrypted() -> pl.DataFrame:
    JSON_FILE = "tests/data/personer_pseudonymized_default_encryption.json"
    return pl.read_json(
        JSON_FILE,
        schema={
            "fnr": pl.String,
            "fornavn": pl.String,
            "etternavn": pl.String,
            "kjonn": pl.String,
            "fodselsdato": pl.String,
        },
    )


@pytest.fixture
def df_pandas_personer_fnr_daead_encrypted() -> pd.DataFrame:
    JSON_FILE = "tests/data/personer_pseudonymized_default_encryption.json"
    return pd.read_json(
        JSON_FILE,
        dtype={
            "fnr": str,
            "fornavn": str,
            "etternavn": str,
            "kjonn": str,
            "fodselsdato": str,
        },
    )


@pytest.fixture()
def single_field_response() -> MagicMock:
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.content = b'{"data": ["Donald","Mikke","Anton"], "datadoc_metadata": {"pseudo_variables": []}, "metrics": [], "logs": []}'
    return mock_response