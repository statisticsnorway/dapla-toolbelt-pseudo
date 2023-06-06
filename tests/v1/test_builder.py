import json
from unittest.mock import MagicMock
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
from dapla_pseudo.v1.supported_file_format import NoFileExtensionError


PKG = "dapla_pseudo.v1.builder"
TEST_FILE_PATH = "tests/v1/test_files"


@pytest.fixture()
def df() -> pd.DataFrame:
    with open("tests/data/personer.json") as test_data:
        return pd.json_normalize(json.load(test_data))


@patch(f"{PKG}._client")
def test_builder_pandas_pseudonymize_minimal_call(patched_client: Mock, df: pd.DataFrame) -> None:
    patched_client.pseudonymize.return_value = requests.Response()
    PseudoData.from_pandas(df).on_field("fornavn").pseudonymize()


def test_builder_fields_selector_single_field(df: pd.DataFrame) -> None:
    PseudoData.from_pandas(df).on_field("fornavn")._fields = [Field(pattern="**/fornavn")]


def test_builder_fields_selector_multiple_fields(df: pd.DataFrame) -> None:
    PseudoData.from_pandas(df).on_fields("fornavn", "fnr")._fields = [
        Field(pattern="**/fornavn"),
        Field(pattern="**/fnr"),
    ]


@patch(f"{PKG}._do_pseudonymization")
def test_builder_pseudo_function_selector_default(patch_do_pseudonymization: MagicMock, df: pd.DataFrame) -> None:
    PseudoData.from_pandas(df).on_field("fornavn").pseudonymize()
    patch_do_pseudonymization.assert_called_once_with(
        dataframe=df,
        fields=[Field(pattern="**/fornavn")],
        pseudo_func=PseudoFunction(function_type=PseudoFunctionTypes.DAEAD, key=PredefinedKeys.SSB_COMMON_KEY_1),
    )


@patch(f"{PKG}._do_pseudonymization")
def test_builder_pseudo_function_selector_map_to_sid(patch_do_pseudonymization: MagicMock, df: pd.DataFrame) -> None:
    PseudoData.from_pandas(df).on_field("fnr").map_to_stable_id().pseudonymize()
    patch_do_pseudonymization.assert_called_once_with(
        dataframe=df,
        fields=[Field(pattern="**/fnr")],
        pseudo_func=PseudoFunction(function_type=PseudoFunctionTypes.MAP_SID, key=PredefinedKeys.PAPIS_COMMON_KEY_1),
    )


@patch(f"{PKG}._do_pseudonymization")
def test_builder_pseudo_function_selector_fpe(patch_do_pseudonymization: MagicMock, df: pd.DataFrame) -> None:
    PseudoData.from_pandas(df).on_field("fnr").pseudonymize(preserve_formatting=True)
    patch_do_pseudonymization.assert_called_once_with(
        dataframe=df,
        fields=[Field(pattern="**/fnr")],
        pseudo_func=PseudoFunction(
            function_type=PseudoFunctionTypes.FF31,
            key=PredefinedKeys.PAPIS_COMMON_KEY_1,
            extra_kwargs=["strategy=SKIP"],
        ),
    )


@patch(f"{PKG}._do_pseudonymization")
def test_builder_pseudo_function_selector_custom(patch_do_pseudonymization: MagicMock, df: pd.DataFrame) -> None:
    pseudo_func = PseudoFunction(function_type=PseudoFunctionTypes.FF31, key=PredefinedKeys.SSB_COMMON_KEY_2)
    PseudoData.from_pandas(df).on_field("fnr").pseudonymize(with_custom_function=pseudo_func)

    patch_do_pseudonymization.assert_called_once_with(
        dataframe=df,
        fields=[Field(pattern="**/fnr")],
        pseudo_func=pseudo_func,
    )


def test_builder_field_selector_multiple_fields(df: pd.DataFrame) -> None:
    fields = ["snr", "snr_mor", "snr_far"]
    assert PseudoData.from_pandas(df).on_fields(*fields)._fields == [Field(pattern=f"**/{f}") for f in fields]


def test_builder_from_file_not_a_file() -> None:
    path = f"{TEST_FILE_PATH}/not/a/file.json"
    with pytest.raises(FileNotFoundError):
        PseudoData.from_file(path)


def test_builder_from_file_no_file_extension() -> None:
    path = f"{TEST_FILE_PATH}/empty_file"
    with pytest.raises(NoFileExtensionError):
        PseudoData.from_file(path)


@patch(f"{PKG}.pd.read_csv")
def test_builder_from_file_with_storage_options(pandas_form_csv: Mock) -> None:
    # This should not raise a FileNotFoundError
    # since the file is not on the local filesystem
    try:
        file_path = "path/to/your/file.csv"
        storage_options = {"token": "fake_token"}
        PseudoData.from_file(file_path, storage_options=storage_options)
    except FileNotFoundError:
        pytest.fail("FileNotFoundError should not be raised when storage_options is supplied.")


@pytest.mark.parametrize(
    "file_format,expected_error",
    [("json", "ValueError"), ("csv", "EmptyDataError"), ("xml", "XMLSyntaxError"), ("parquet", "ArrowInvalid")],
)
@patch("pathlib.Path.suffix")
def test_builder_from_file_empty_file(mock_path_suffix: Mock, file_format: str, expected_error: str) -> None:
    mock_path_suffix.__getitem__.return_value = file_format

    path = f"{TEST_FILE_PATH}/empty_file"

    with pytest.raises(Exception) as e:
        PseudoData.from_file(path)

    # Check that the appropriate errors for the given filetype are raised.
    assert e.typename == expected_error
    mock_path_suffix.__getitem__.assert_called_once()


@pytest.mark.parametrize("file_format", ["json", "csv", "xml", "parquet"])
def test_builder_from_file(file_format: str) -> None:
    # Test reading all supported file extensions
    PseudoData.from_file(f"{TEST_FILE_PATH}/test.{file_format}")
