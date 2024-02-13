import json
import os
import subprocess
from collections.abc import Generator

import polars as pl
import pytest
from datadoc_model.model import MetadataContainer


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


def integration_test() -> pytest.MarkDecorator:
    # Tests annotated with integration_test will run if `INTEGRATION_TESTS` env variable is unset or `TRUE`
    # This is used to disable integration tests in the `test.yaml` workflow, since these tests need additional configuration.
    enabled = os.environ.get("INTEGRATION_TESTS", "TRUE")
    return pytest.mark.skipif(
        enabled != "TRUE", reason="Integration tests are disabled for current test"
    )


@pytest.fixture()
def setup() -> Generator[None, None, None]:
    os.environ["PSEUDO_SERVICE_URL"] = (
        "https://dapla-pseudo-service.staging-bip-app.ssb.no"
    )
    # Setup step that runs when integration test are ran on local machine
    # This will not run in GH actions since GITHUB_ACTIONS is set to `true` per default
    # https://docs.github.com/en/actions/learn-github-actions/variables
    if os.environ.get("GITHUB_ACTIONS") != "true":
        # Setup step
        # Could not find a way to generate id tokes without a SA to impersonate.
        # Subprocess `glcoud auth` as a temporary workaround
        id_token = subprocess.getoutput("gcloud auth print-identity-token")
        os.environ["PSEUDO_SERVICE_AUTH_TOKEN"] = id_token
        yield
        # Teardown step
        os.unsetenv("PSEUDO_SERVICE_URL")
        os.unsetenv("PSEUDO_SERVICE_AUTH_TOKEN")
    else:
        # If ran from GitHub action setup is required
        yield


def get_expected_datadoc_metadata_container(
    calling_function_name: str,
) -> MetadataContainer:
    """Helper function that returns the expected MetadataContainer for a given calling function."""
    expected_metadata_json_file = (
        f"tests/data/datadoc/expected_metadata_{calling_function_name}.json"
    )
    with open(expected_metadata_json_file) as json_file:
        expected_datadoc_json = json.load(json_file)
    return MetadataContainer(**expected_datadoc_json)
