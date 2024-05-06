from datetime import date
from unittest.mock import MagicMock
from unittest.mock import Mock
from unittest.mock import patch

import pandas as pd
import polars as pl
import pytest
import pytest_cases

from dapla_pseudo.exceptions import NoFileExtensionError
from dapla_pseudo.utils import convert_to_date
from dapla_pseudo.v1.validation import Validator

PKG = "dapla_pseudo.v1.validation"
TEST_FILE_PATH = "tests/v1/unit/test_files"


@pytest_cases.fixture()
def sid_lookup_missing_response() -> MagicMock:
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.content = b'[{"missing": ["20859374701","01234567890"], "datasetExtractionSnapshotTime": "2023-08-31"}]'
    return mock_response


@pytest_cases.fixture()
def sid_lookup_empty_response() -> MagicMock:
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.content = b'[{"datasetExtractionSnapshotTime": "2023-08-31"}]'
    return mock_response


@patch("dapla_pseudo.v1.PseudoClient._post_to_sid_endpoint")
def test_validate_with_full_response(
    patched_post_to_sid_endpoint: Mock,
    df_personer: pl.DataFrame,
    sid_lookup_missing_response: MagicMock,
) -> None:
    field_name = "fnr"

    patched_post_to_sid_endpoint.return_value = sid_lookup_missing_response

    validation_result = (
        Validator.from_polars(df_personer)
        .on_field(field_name)
        .validate_map_to_stable_id()
    )
    validation_df = validation_result.to_pandas()
    validation_metadata = validation_result.metadata_details

    patched_post_to_sid_endpoint.assert_called_once_with(
        "sid/lookup/batch",
        ["11854898347", "01839899544", "16910599481"],
        None,
        stream=True,
    )
    assert validation_df[field_name].tolist() == ["20859374701", "01234567890"]
    assert validation_metadata[field_name]["logs"] == ["SID snapshot time 2023-08-31"]


@patch("dapla_pseudo.v1.PseudoClient._post_to_sid_endpoint")
def test_validate_with_empty_response(
    patched_post_to_sid_endpoint: Mock,
    df_personer: pl.DataFrame,
    sid_lookup_empty_response: MagicMock,
) -> None:
    field_name = "fnr"

    patched_post_to_sid_endpoint.return_value = sid_lookup_empty_response

    validation_result = (
        Validator.from_polars(df_personer)
        .on_field(field_name)
        .validate_map_to_stable_id(sid_snapshot_date=convert_to_date("2023-08-31"))
    )
    validation_df = validation_result.to_pandas()
    validation_metadata = validation_result.metadata_details

    patched_post_to_sid_endpoint.assert_called_once_with(
        "sid/lookup/batch",
        ["11854898347", "01839899544", "16910599481"],
        date(2023, 8, 31),
        stream=True,
    )
    assert validation_df[field_name].tolist() == []
    assert validation_metadata[field_name]["logs"] == ["SID snapshot time 2023-08-31"]


def test_builder_from_file_not_a_file() -> None:
    path = f"{TEST_FILE_PATH}/not/a/file.json"
    with pytest.raises(FileNotFoundError):
        Validator.from_file(path)


def test_builder_from_file_no_file_extension() -> None:
    path = f"{TEST_FILE_PATH}/empty_file"
    with pytest.raises(NoFileExtensionError):
        Validator.from_file(path)


@patch(f"{PKG}.read_to_polars_df")
def test_builder_from_file_with_storage_options(_mock_read_to_pandas_df: Mock) -> None:
    # This should not raise a FileNotFoundError
    # since the file is not on the local filesystem
    try:
        file_path = "path/to/your/file.csv"
        storage_options = {"token": "fake_token"}
        Validator.from_file(file_path, storage_options=storage_options)
    except FileNotFoundError:
        pytest.fail(
            "FileNotFoundError should not be raised when storage_options is supplied."
        )


def test_builder_from_polars(df_personer_pandas: pd.DataFrame) -> None:
    Validator.from_polars(pl.from_pandas(df_personer_pandas))
