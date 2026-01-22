import polars as pl
import pytest
import requests
from tests.v1.integration.utils import integration_test

from dapla_pseudo import Validator
from dapla_pseudo.v1.result import Result


@pytest.mark.usefixtures("setup")
@integration_test()
def test_sid_lookup_batch_payload_too_large() -> None:
    n_rows = 1_400_000
    df = pl.DataFrame({"fnr": [f"{i:011d}" for i in range(n_rows)]})

    try:
        result = Validator.from_polars(df).on_field("fnr").validate_map_to_stable_id()
    except requests.HTTPError as err:
        # Explicitly fail on HTTP 413
        if err.response is not None and err.response.status_code == 413:
            pytest.fail(f"Unexpected 413 Payload Too Large: {err.response.text}")
        # Re-raise other HTTP errors to surface genuine failures
        raise
    except requests.ConnectionError as err:
        msg = str(err)
        # Some servers may close connection with a payload-too-large reason
        if ("Payload Too Large" in msg) or ("Request Entity Too Large" in msg):
            pytest.fail(f"Unexpected payload-too-large connection error: {msg}")
        raise

    # Sanity check: we got a Result back
    assert isinstance(result, Result)
