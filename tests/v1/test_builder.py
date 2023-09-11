import json
import typing as t
from unittest.mock import MagicMock
from unittest.mock import Mock
from unittest.mock import patch

import pandas as pd
import polars as pl
import pytest

from dapla_pseudo.constants import PredefinedKeys
from dapla_pseudo.constants import PseudoFunctionTypes
from dapla_pseudo.v1.builder import PseudoData
from dapla_pseudo.v1.builder import _do_pseudonymize_field
from dapla_pseudo.v1.models import PseudoFunction
from dapla_pseudo.v1.models import PseudoKeyset
from dapla_pseudo.v1.models import MapSidKeywordArgs, FF31KeywordArgs, DaeadKeywordArgs
from dapla_pseudo.v1.supported_file_format import NoFileExtensionError


PKG = "dapla_pseudo.v1.builder"
TEST_FILE_PATH = "tests/v1/test_files"


@pytest.fixture()
def df() -> pd.DataFrame:
    with open("tests/data/personer.json") as test_data:
        return pd.json_normalize(json.load(test_data))


@pytest.fixture()
def single_field_response() -> MagicMock:
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.content = b'[["f1","f2","f3"]]'
    mock_response.headers = {
        "metadata": '{"fieldName":"fornavn","pseudoRules":{"rules":[{"name":"fornavn","pattern":"**","func":"daead(keyId=fake-ssb-key)"}],"keysets":[]}}'
    }
    return mock_response


def mock_return_do_pseudonymize_field(patch_do_pseudonymize_field: Mock) -> None:
    patch_do_pseudonymize_field.return_value = pl.Series(["e1", "e2", "e3"])


@patch("dapla_pseudo.v1.PseudoClient._post_to_field_endpoint")
def test_builder_pandas_pseudonymize_minimal_call(
    patched_post_to_field_endpoint: Mock, df: pd.DataFrame, single_field_response: MagicMock
) -> None:
    field_name = "fornavn"

    patched_post_to_field_endpoint.return_value = single_field_response

    pseudo_result = PseudoData.from_pandas(df).on_field(field_name).pseudonymize()
    pseudo_dataframe = pseudo_result.to_pandas()
    pseudo_metadata = pseudo_result.get_metadata()

    # Check that the pseudonymized df has new values
    assert pseudo_dataframe[field_name].tolist() == ["f1", "f2", "f3"]
    assert (
        pseudo_metadata[field_name]
        == '{"fieldName":"fornavn","pseudoRules":{"rules":[{"name":"fornavn","pattern":"**","func":"daead(keyId=fake-ssb-key)"}],"keysets":[]}}'
    )


@patch("dapla_pseudo.v1.PseudoClient._post_to_field_endpoint")
def test_single_field_do_pseudonymize_field(
    patched_post_to_field_endpoint: Mock, single_field_response: MagicMock
) -> None:
    patched_post_to_field_endpoint.return_value = single_field_response

    pseudo_func = PseudoFunction(function_type=PseudoFunctionTypes.MAP_SID.value, key="fake-key", kwargs=MapSidKeywordArgs())
    metadata: t.Dict[str, str] = dict()
    series: pl.Series = _do_pseudonymize_field("fake.endpoint", "fornavn", ["x1", "x2", "x3"], pseudo_func, metadata)
    assert series.to_list() == ["f1", "f2", "f3"]


def test_builder_fields_selector_single_field(df: pd.DataFrame) -> None:
    PseudoData.from_pandas(df).on_field("fornavn")._fields = ["fornavn"]


def test_builder_fields_selector_multiple_fields(df: pd.DataFrame) -> None:
    PseudoData.from_pandas(df).on_fields("fornavn", "fnr")._fields = [
        "fornavn",
        "fnr",
    ]


@patch(f"{PKG}._do_pseudonymize_field")
def test_builder_pseudo_function_selector_default(patch_do_pseudonymize_field: MagicMock, df: pd.DataFrame) -> None:
    mock_return_do_pseudonymize_field(patch_do_pseudonymize_field)
    PseudoData.from_pandas(df).on_field("fornavn").pseudonymize()
    patch_do_pseudonymize_field.assert_called_once_with(
        path="pseudonymize/field",
        field_name="fornavn",
        values=df["fornavn"].tolist(),
        pseudo_func=PseudoFunction(function_type=PseudoFunctionTypes.DAEAD, key=PredefinedKeys.SSB_COMMON_KEY_1, kwargs=DaeadKeywordArgs()),
        metadata_map={},
        keyset=None,
    )


@patch(f"{PKG}._do_pseudonymize_field")
def test_builder_pseudo_function_selector_map_to_sid(patch_do_pseudonymize_field: MagicMock, df: pd.DataFrame) -> None:
    mock_return_do_pseudonymize_field(patch_do_pseudonymize_field)
    PseudoData.from_pandas(df).on_field("fnr").map_to_stable_id().pseudonymize()
    patch_do_pseudonymize_field.assert_called_once_with(
        path="pseudonymize/field",
        values=df["fnr"].tolist(),
        field_name="fnr",
        pseudo_func=PseudoFunction(
            function_type=PseudoFunctionTypes.MAP_SID, 
            key=PredefinedKeys.SSB_COMMON_KEY_1,
            kwargs=MapSidKeywordArgs()
        ),
        metadata_map={},
        keyset=None,
    )


@patch(f"{PKG}._do_pseudonymize_field")
def test_builder_pseudo_function_selector_fpe(patch_do_pseudonymize_field: MagicMock, df: pd.DataFrame) -> None:
    mock_return_do_pseudonymize_field(patch_do_pseudonymize_field)
    PseudoData.from_pandas(df).on_field("fnr").pseudonymize(preserve_formatting=True)
    patch_do_pseudonymize_field.assert_called_once_with(
        path="pseudonymize/field",
        values=df["fnr"].tolist(),
        field_name="fnr",
        pseudo_func=PseudoFunction(
            function_type=PseudoFunctionTypes.FF31,
            key=PredefinedKeys.PAPIS_COMMON_KEY_1,
            kwargs=FF31KeywordArgs()
        ),
        metadata_map={},
        keyset=None,
    )


@patch(f"{PKG}._do_pseudonymize_field")
def test_builder_pseudo_function_selector_custom(patch_do_pseudonymize_field: MagicMock, df: pd.DataFrame) -> None:
    mock_return_do_pseudonymize_field(patch_do_pseudonymize_field)
    pseudo_func = PseudoFunction(
        function_type=PseudoFunctionTypes.FF31, 
        key=PredefinedKeys.SSB_COMMON_KEY_2,
        kwargs=FF31KeywordArgs()
    )
    PseudoData.from_pandas(df).on_field("fnr").pseudonymize(with_custom_function=pseudo_func)

    patch_do_pseudonymize_field.assert_called_once_with(
        path="pseudonymize/field",
        values=df["fnr"].tolist(),
        field_name="fnr",
        pseudo_func=pseudo_func,
        metadata_map={},
        keyset=None,
    )


@patch(f"{PKG}._do_pseudonymize_field")
def test_builder_pseudo_keyset_selector_custom(patch_do_pseudonymize_field: MagicMock, df: pd.DataFrame) -> None:
    mock_return_do_pseudonymize_field(patch_do_pseudonymize_field)

    kek_uri = "gcp-kms://fake/pseudo-service-fake"
    encrypted_keyset = "fake_keyset"
    keyset_info = {
        "primaryKeyId": 123,
        "keyInfo": [
            {
                "typeUrl": "type.googleapis.com/google.crypto.tink.fake",
                "status": "ENABLED",
                "keyId": 123,
                "outputPrefixType": "TINK",
            }
        ],
    }
    pseudo_func = PseudoFunction(
        function_type=PseudoFunctionTypes.DAEAD, 
        key="1403797237",
        kwargs=DaeadKeywordArgs()
    )
    keyset = PseudoKeyset(kek_uri=kek_uri, encrypted_keyset=encrypted_keyset, keyset_info=keyset_info)

    PseudoData.from_pandas(df).on_field("fnr").pseudonymize(with_custom_function=pseudo_func, with_custom_keyset=keyset)

    patch_do_pseudonymize_field.assert_called_once_with(
        path="pseudonymize/field",
        values=df["fnr"].tolist(),
        field_name="fnr",
        pseudo_func=pseudo_func,
        metadata_map={},
        keyset=keyset,
    )


@patch(f"{PKG}._do_pseudonymize_field")
def test_pseudonymize_field_dataframe_setup(patch_do_pseudonymize_field: MagicMock, df: pd.DataFrame) -> None:
    def side_effect(**kwargs: t.Any) -> pl.Series:
        name = kwargs["field_name"]
        return pl.Series([f"{name}1", f"{name}2", f"{name}3"])

    patch_do_pseudonymize_field.side_effect = side_effect

    fields_to_pseudonymize = "fnr", "fornavn", "etternavn"
    dataframe = PseudoData.from_pandas(df).on_fields(*fields_to_pseudonymize).pseudonymize().to_pandas()

    for field in fields_to_pseudonymize:
        assert dataframe[field].to_list() == side_effect(field_name=field).to_list()


def test_builder_field_selector_multiple_fields(df: pd.DataFrame) -> None:
    fields = ["snr", "snr_mor", "snr_far"]
    assert PseudoData.from_pandas(df).on_fields(*fields)._fields == [f"{f}" for f in fields]


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


@patch(f"{PKG}._do_pseudonymize_field")
def test_builder_to_polars_from_polars_chaining(patch_do_pseudonymize_field: MagicMock, df: pd.DataFrame) -> None:
    def side_effect(**kwargs: t.Any) -> pl.Series:
        name = kwargs["field_name"]
        return pl.Series([f"{name}1", f"{name}2", f"{name}3"])

    patch_do_pseudonymize_field.side_effect = side_effect
    fields_to_pseudonymize = "fnr", "fornavn", "etternavn"
    result: pl.DataFrame = PseudoData.from_pandas(df).on_fields(*fields_to_pseudonymize).pseudonymize().to_polars()
    assert PseudoData.from_polars(result).on_field("fnr").map_to_stable_id().pseudonymize().to_polars() is not None
