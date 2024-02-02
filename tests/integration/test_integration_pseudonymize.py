import os
import subprocess
from collections.abc import Generator

import polars as pl
import pytest

from dapla_pseudo import Pseudonymize
from tests.integration.utils import integration_test

JSON_FILE = "tests/data/personer.json"


@pytest.fixture
def setup() -> Generator[None, None, None]:
    # Setup step that runs when integration test are ran on local machine
    if os.environ.get("DAPLA_REGION") is None:
        # Could not find a way to generate id tokes without a SA to impersonate.
        # https://google-auth.readthedocs.io/en/master/reference/google.oauth2.id_token.html
        # Subprocessing `glcoud auth` as a temporary workaround
        id_token = subprocess.getoutput("gcloud auth print-identity-token")
        os.environ[
            "PSEUDO_SERVICE_URL"
        ] = "https://dapla-pseudo-service.staging-bip-app.ssb.no"
        os.environ["PSEUDO_SERVICE_AUTH_TOKEN"] = id_token
        yield
        os.unsetenv("PSEUDO_SERVICE_URL")
        os.unsetenv("PSEUDO_SERVICE_AUTH_TOKEN")


@pytest.fixture(scope="module")
def df_personer() -> pl.DataFrame:
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


@integration_test()
def test_pseudonymize_default_encryption(
    setup: Generator[None, None, None], df_personer: pl.DataFrame
) -> None:
    expected_result_fnr_df = pl.DataFrame(
        {
            "fnr": [
                "AWIRfKLSNfR0ID+wBzogEcUT7JQPayk7Gosij6SXr8s=",
                "AWIRfKKLagk0LqYCKpiC4xfPkHqIWGVfc3wg5gUwRNE=",
                "AWIRfKIzL1T9iZqt+pLjNbHMsLa0aKSszsRrLiLSAAg=",
            ]
        }
    )
    expected_result_df = df_personer.clone().update(expected_result_fnr_df)
    result = (
        Pseudonymize.from_polars(df_personer)  # Select dataset
        .on_fields("fnr")  # Select fields in dataset
        .with_default_encryption()  # Select encryption method on fields
        .run()  # Apply pseudonymization
    )
    assert result.to_polars().equals(expected_result_df)


@integration_test()
def test_pseudonymize_sid(
    setup: Generator[None, None, None], df_personer: pl.DataFrame
) -> None:
    expected_result_fnr_df = pl.DataFrame(
        {
            "fnr": [
                "jJuuj0i",
                "ylc9488",
                "yeLfkaL",
            ]
        }
    )
    expected_result_df = df_personer.clone().update(expected_result_fnr_df)
    result = (
        Pseudonymize.from_polars(df_personer)  # Select dataset
        .on_fields("fnr")  # Select fields in dataset
        .with_stable_id()  # Select encryption method on fields
        .run()  # Apply pseudonymization
    )
    assert result.to_polars().equals(expected_result_df)
