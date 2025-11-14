from unittest.mock import MagicMock

import pandas as pd
import polars as pl
import pytest_cases
from dapla_metadata.datasets.core import Datadoc


@pytest_cases.fixture()
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


@pytest_cases.fixture()
def df_personer_metadata() -> Datadoc:
    JSON_FILE = "tests/data/personer_metadata.json"
    return Datadoc(metadata_document_path=JSON_FILE, errors_as_warnings=True)


@pytest_cases.fixture()
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


@pytest_cases.fixture()
def personer_hierarch_file_path() -> str:
    return "tests/data/personer_hierarchical.json"


@pytest_cases.fixture()
def personer_pseudonymized_hierarch_file_path() -> str:
    return "tests/data/personer_hierarchical_pseudonymized.json"


@pytest_cases.fixture()
def personer_file_path() -> str:
    return "tests/data/personer.json"


@pytest_cases.fixture()
def personer_pseudonymized_file_path() -> str:
    return "tests/data/personer_pseudonymized_default_encryption.json"


@pytest_cases.fixture()
def df_personer_hierarchical() -> pl.DataFrame:
    JSON_FILE = "tests/data/personer_hierarchical.json"
    return pl.read_json(
        JSON_FILE,
        schema={
            "person_info": pl.Struct(
                [
                    pl.Field("fnr", dtype=pl.String),
                    pl.Field("fornavn", dtype=pl.String),
                    pl.Field("etternavn", dtype=pl.String),
                ]
            ),
            "kjonn": pl.String,
            "fodselsdato": pl.String,
        },
    )


@pytest_cases.fixture()
def df_personer_hierarchical_pseudonymized() -> pl.DataFrame:
    JSON_FILE = "tests/data/personer_hierarchical_pseudonymized.json"
    return pl.read_json(
        JSON_FILE,
        schema={
            "person_info": pl.Struct(
                [
                    pl.Field("fnr", dtype=pl.String),
                    pl.Field("fornavn", dtype=pl.String),
                    pl.Field("etternavn", dtype=pl.String),
                ]
            ),
            "kjonn": pl.String,
            "fodselsdato": pl.String,
        },
    )


@pytest_cases.fixture()
def df_personer_hierarchical_redacted() -> pl.DataFrame:
    JSON_FILE = "tests/data/personer_hierarchical_redacted.json"
    return pl.read_json(
        JSON_FILE,
        schema={
            "person_info": pl.Struct(
                [
                    pl.Field("fnr", dtype=pl.String),
                    pl.Field("fornavn", dtype=pl.String),
                    pl.Field("etternavn", dtype=pl.String),
                ]
            ),
            "kjonn": pl.String,
            "fodselsdato": pl.String,
        },
    )


@pytest_cases.fixture()
def df_personer_hierarchical_null() -> pl.DataFrame:
    JSON_FILE = "tests/data/personer_hierarchical_null.json"
    return pl.read_json(
        JSON_FILE,
    )


@pytest_cases.fixture()
def df_personer_hierarchical_null_pseudonymized() -> pl.DataFrame:
    JSON_FILE = "tests/data/personer_hierarchical_null_pseudonymized.json"
    return pl.read_json(
        JSON_FILE,
    )


@pytest_cases.fixture()
def df_personer_hierarchical_inner_list() -> pl.DataFrame:
    JSON_FILE = "tests/data/personer_hierarchical_inner_list.json"
    return pl.read_json(
        JSON_FILE,
    )


@pytest_cases.fixture()
def df_personer_hierarchical_inner_list_pseudonymized() -> pl.DataFrame:
    JSON_FILE = "tests/data/personer_hierarchical_inner_list_pseudonymized.json"
    return pl.read_json(
        JSON_FILE,
    )


@pytest_cases.fixture()
def df_personer_hierarchical_complex() -> pl.DataFrame:
    JSON_FILE = "tests/data/personer_hierarchical_complex.json"
    return pl.read_json(
        JSON_FILE,
    )


@pytest_cases.fixture()
def df_personer_hierarchical_complex_pseudonymized() -> pl.DataFrame:
    JSON_FILE = "tests/data/personer_hierarchical_complex_pseudonymized.json"
    return pl.read_json(
        JSON_FILE,
    )


@pytest_cases.fixture()
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


@pytest_cases.fixture()
def df_personer_fnr_daead_encrypted_metadata() -> Datadoc:
    JSON_FILE = "tests/data/personer_pseudonymized_default_encryption_metadata.json"
    return Datadoc(metadata_document_path=JSON_FILE, errors_as_warnings=True)


@pytest_cases.fixture()
def df_personer_fnr_ff31_encrypted() -> pl.DataFrame:
    JSON_FILE = "tests/data/personer_pseudonymized_papis_compatible_encryption.json"
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


@pytest_cases.fixture()
def df_personer_daead_encrypted_ssb_common_key_1() -> pl.DataFrame:
    JSON_FILE = "tests/data/personer_pseudonymized_daead_ssb_common_key_1.json"
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


@pytest_cases.fixture()
def df_personer_daead_encrypted_ssb_common_key_2() -> pl.DataFrame:
    JSON_FILE = "tests/data/personer_pseudonymized_daead_ssb_common_key_2.json"
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


@pytest_cases.fixture()
def df_personer_pseudo_stable_id_daead_encrypted_ssb_common_key_2() -> pl.DataFrame:
    JSON_FILE = "tests/data/personer_pseudonymized_sid_daead_ssb_common_key_2.json"
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


@pytest_cases.fixture()
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


@pytest_cases.fixture()
def df_personer_sid_fnr() -> pl.DataFrame:
    JSON_FILE = "tests/data/personer_pseudonymized_sid_fnr.json"
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


@pytest_cases.fixture()
def pandas_diverse_datatypes() -> pd.DataFrame:
    JSON_FILE = "tests/data/diverse_datatypes.json"
    return pd.read_json(
        JSON_FILE,
        dtype={
            "string_field": "string[pyarrow]",
            "int_field": "Int64[pyarrow]",
            "float_field": "Float64[pyarrow]",
            "date_pseudonymized": "datetime64[s]",
            "bool_field": "boolean[pyarrow]",
        },
        dtype_backend="pyarrow",
    )


@pytest_cases.fixture()
def single_field_response() -> MagicMock:
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.content = b'{"data": ["Donald","Mikke","Anton"], "datadoc_metadata": {"pseudo_variables": []}, "metrics": [], "logs": []}'
    return mock_response
