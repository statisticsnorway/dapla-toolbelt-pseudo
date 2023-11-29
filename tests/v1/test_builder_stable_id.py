import json
from unittest.mock import MagicMock
from unittest.mock import Mock
from unittest.mock import patch

import pandas as pd
import pytest

from dapla_pseudo.v1.builder_stable_id import Validator


PKG = "dapla_pseudo.v1.builder_pseudo"
TEST_FILE_PATH = "tests/v1/test_files"


@pytest.fixture()
def df() -> pd.DataFrame:
    with open("tests/data/personer.json") as test_data:
        return pd.json_normalize(json.load(test_data))


@pytest.fixture()
def sid_lookup_missing_response() -> MagicMock:
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.content = (
        b'[{"missing": ["20859374701","01234567890"], ' b'"datasetExtractionSnapshotTime": "2023-08-31"}]'
    )
    return mock_response


@patch("dapla_pseudo.v1.PseudoClient._post_to_sid_endpoint")
def test_builder_validate_map_to_stable_id(
    patched_post_to_sid_endpoint: Mock, df: pd.DataFrame, sid_lookup_missing_response: MagicMock
) -> None:
    field_name = "fnr"

    patched_post_to_sid_endpoint.return_value = sid_lookup_missing_response

    validation_result = Validator.from_pandas(df).on_field(field_name).validate_map_to_stable_id()
    validation_df = validation_result.to_pandas()
    validation_metadata = validation_result.metadata

    assert validation_df[field_name].tolist() == ["20859374701", "01234567890"]
    assert validation_metadata == {"datasetExtractionSnapshotTime": "2023-08-31"}
