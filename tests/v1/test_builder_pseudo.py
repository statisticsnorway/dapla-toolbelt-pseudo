import json
import typing as t
from datetime import date
from unittest.mock import MagicMock
from unittest.mock import Mock
from unittest.mock import patch
from mock import ANY

import pandas as pd
import polars as pl
import pytest

from dapla_pseudo.constants import TIMEOUT_DEFAULT, PseudoFunctionTypes
from dapla_pseudo.utils import convert_to_date
from dapla_pseudo.v1.builder_models import Result
from dapla_pseudo.v1.builder_pseudo import File, PseudoData, _get_content_type_from_file
from dapla_pseudo.v1.builder_pseudo import _do_pseudonymize_field
from dapla_pseudo.v1.models import (
    DaeadKeywordArgs,
    KeyWrapper,
    Mimetypes,
    PseudoConfig,
    PseudoRule,
    PseudonymizeFileRequest,
)
from dapla_pseudo.v1.models import FF31KeywordArgs
from dapla_pseudo.v1.models import MapSidKeywordArgs
from dapla_pseudo.v1.models import PseudoFunction
from dapla_pseudo.v1.models import PseudoKeyset
from dapla_pseudo.v1.models import RedactArgs
from dapla_pseudo.exceptions import MimetypeNotSupportedError, NoFileExtensionError
from dapla_pseudo.v1.supported_file_format import SupportedFileFormat

PKG = "dapla_pseudo.v1.builder_pseudo"
TEST_FILE_PATH = "tests/v1/test_files"


@pytest.fixture()
def df() -> pd.DataFrame:
    with open("tests/data/personer.json") as test_data:
        return pd.json_normalize(json.load(test_data))


@pytest.fixture()
def json_file_path() -> pd.DataFrame:
    return "tests/data/personer.json"


@pytest.fixture()
def json_hierarch_file_path() -> pd.DataFrame:
    return "tests/data/personer_hierarchical.json"


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

    pseudo_result = PseudoData.from_pandas(df).on_fields(field_name).with_default_encryption().pseudonymize()
    assert isinstance(pseudo_result, Result)
    pseudo_dataframe = pseudo_result.to_pandas()
    pseudo_metadata = pseudo_result.metadata

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

    pseudo_func = PseudoFunction(function_type=PseudoFunctionTypes.MAP_SID, kwargs=MapSidKeywordArgs(key_id="fake-key"))
    metadata: t.Dict[str, str] = dict()
    series: pl.Series = _do_pseudonymize_field(
        "fake.endpoint", "fornavn", ["x1", "x2", "x3"], pseudo_func, metadata, TIMEOUT_DEFAULT
    )
    assert series.to_list() == ["f1", "f2", "f3"]


def test_builder_fields_selector_single_field(df: pd.DataFrame) -> None:
    PseudoData.from_pandas(df).on_fields("fornavn")._fields = ["fornavn"]


def test_builder_fields_selector_multiple_fields(df: pd.DataFrame) -> None:
    PseudoData.from_pandas(df).on_fields("fornavn", "fnr")._fields = [
        "fornavn",
        "fnr",
    ]


@patch("dapla_pseudo.v1.PseudoClient.pseudonymize_file")
def test_builder_file_default(patched_pseudonymize_file: MagicMock, json_file_path: str):
    patched_pseudonymize_file.return_value = Mock()
    PseudoData.from_file(json_file_path).on_fields("fornavn").with_default_encryption().pseudonymize()

    pseudonymize_request = PseudonymizeFileRequest(
        pseudo_config=PseudoConfig(
            rules=[
                PseudoRule(
                    name=None,
                    pattern="**/fornavn",
                    func=PseudoFunction(function_type=PseudoFunctionTypes.DAEAD, kwargs=DaeadKeywordArgs()),
                )
            ],
            keysets=KeyWrapper(None).keyset_list(),
        ),
        target_content_type=Mimetypes.JSON,
        target_uri=None,
        compression=None,
    )
    patched_pseudonymize_file.assert_called_once_with(
        pseudonymize_request,  # use ANY to avoid having to mock the whole request
        PseudoData.dataset.file_handle,
        stream=True,
        name=None,
        timeout=30,
    )


@patch("dapla_pseudo.v1.PseudoClient.pseudonymize_file")
def test_builder_file_hierarchical(patched_pseudonymize_file: MagicMock, json_hierarch_file_path: str):
    patched_pseudonymize_file.return_value = Mock()
    PseudoData.from_file(json_hierarch_file_path).on_fields("person_info/fnr").with_default_encryption().pseudonymize()

    pseudonymize_request = PseudonymizeFileRequest(
        pseudo_config=PseudoConfig(
            rules=[
                PseudoRule(
                    name=None,
                    pattern="**/person_info/fnr",
                    func=PseudoFunction(function_type=PseudoFunctionTypes.DAEAD, kwargs=DaeadKeywordArgs()),
                )
            ],
            keysets=KeyWrapper(None).keyset_list(),
        ),
        target_content_type=Mimetypes.JSON,
        target_uri=None,
        compression=None,
    )
    patched_pseudonymize_file.assert_called_once_with(
        pseudonymize_request,  # use ANY to avoid having to mock the whole request
        PseudoData.dataset.file_handle,
        stream=True,
        name=None,
        timeout=30,
    )


@patch(f"{PKG}._do_pseudonymize_field")
def test_builder_pseudo_function_selector_default(patch_do_pseudonymize_field: MagicMock, df: pd.DataFrame) -> None:
    mock_return_do_pseudonymize_field(patch_do_pseudonymize_field)
    PseudoData.from_pandas(df).on_fields("fornavn").with_default_encryption().pseudonymize()
    patch_do_pseudonymize_field.assert_called_once_with(
        path="pseudonymize/field",
        field_name="fornavn",
        values=df["fornavn"].tolist(),
        pseudo_func=PseudoFunction(function_type=PseudoFunctionTypes.DAEAD, kwargs=DaeadKeywordArgs()),
        metadata_map={},
        timeout=TIMEOUT_DEFAULT,
        keyset=None,
    )


@patch(f"{PKG}._do_pseudonymize_field")
def test_builder_pseudo_function_selector_with_sid(patch_do_pseudonymize_field: MagicMock, df: pd.DataFrame) -> None:
    mock_return_do_pseudonymize_field(patch_do_pseudonymize_field)
    PseudoData.from_pandas(df).on_fields("fnr").with_stable_id().pseudonymize()
    patch_do_pseudonymize_field.assert_called_once_with(
        path="pseudonymize/field",
        values=df["fnr"].tolist(),
        field_name="fnr",
        pseudo_func=PseudoFunction(function_type=PseudoFunctionTypes.MAP_SID, kwargs=MapSidKeywordArgs()),
        metadata_map={},
        timeout=TIMEOUT_DEFAULT,
        keyset=None,
    )


@patch(f"{PKG}._do_pseudonymize_field")
def test_builder_pseudo_function_with_sid_snapshot_date_string(
    patch_do_pseudonymize_field: MagicMock, df: pd.DataFrame
) -> None:
    mock_return_do_pseudonymize_field(patch_do_pseudonymize_field)
    PseudoData.from_pandas(df).on_fields("fnr").with_stable_id(
        sid_snapshot_date=convert_to_date("2023-05-21")
    ).pseudonymize()
    patch_do_pseudonymize_field.assert_called_once_with(
        path="pseudonymize/field",
        values=df["fnr"].tolist(),
        field_name="fnr",
        pseudo_func=PseudoFunction(
            function_type=PseudoFunctionTypes.MAP_SID,
            kwargs=MapSidKeywordArgs(snapshot_date=convert_to_date("2023-05-21")),
        ),
        metadata_map={},
        timeout=TIMEOUT_DEFAULT,
        keyset=None,
    )


@patch(f"{PKG}._do_pseudonymize_field")
def test_builder_pseudo_function_with_sid_snapshot_date_date(
    patch_do_pseudonymize_field: MagicMock, df: pd.DataFrame
) -> None:
    mock_return_do_pseudonymize_field(patch_do_pseudonymize_field)
    PseudoData.from_pandas(df).on_fields("fnr").with_stable_id(
        sid_snapshot_date=date.fromisoformat("2023-05-21")
    ).pseudonymize()
    patch_do_pseudonymize_field.assert_called_once_with(
        path="pseudonymize/field",
        values=df["fnr"].tolist(),
        field_name="fnr",
        pseudo_func=PseudoFunction(
            function_type=PseudoFunctionTypes.MAP_SID,
            kwargs=MapSidKeywordArgs(snapshot_date=date.fromisoformat("2023-05-21")),
        ),
        metadata_map={},
        timeout=TIMEOUT_DEFAULT,
        keyset=None,
    )


@patch(f"{PKG}._do_pseudonymize_field")
def test_builder_pseudo_function_selector_fpe(patch_do_pseudonymize_field: MagicMock, df: pd.DataFrame) -> None:
    mock_return_do_pseudonymize_field(patch_do_pseudonymize_field)
    PseudoData.from_pandas(df).on_fields("fnr").with_papis_compatible_encryption().pseudonymize()
    patch_do_pseudonymize_field.assert_called_once_with(
        path="pseudonymize/field",
        values=df["fnr"].tolist(),
        field_name="fnr",
        pseudo_func=PseudoFunction(function_type=PseudoFunctionTypes.FF31, kwargs=FF31KeywordArgs()),
        metadata_map={},
        timeout=TIMEOUT_DEFAULT,
        keyset=None,
    )


@patch(f"{PKG}._do_pseudonymize_field")
def test_builder_pseudo_function_selector_custom(patch_do_pseudonymize_field: MagicMock, df: pd.DataFrame) -> None:
    mock_return_do_pseudonymize_field(patch_do_pseudonymize_field)
    pseudo_func = PseudoFunction(function_type=PseudoFunctionTypes.FF31, kwargs=FF31KeywordArgs())
    PseudoData.from_pandas(df).on_fields("fnr").with_custom_function(pseudo_func).pseudonymize()

    patch_do_pseudonymize_field.assert_called_once_with(
        path="pseudonymize/field",
        values=df["fnr"].tolist(),
        field_name="fnr",
        pseudo_func=pseudo_func,
        metadata_map={},
        timeout=TIMEOUT_DEFAULT,
        keyset=None,
    )


@patch(f"{PKG}._do_pseudonymize_field")
def test_builder_pseudo_function_selector_redact(patch_do_pseudonymize_field: MagicMock, df: pd.DataFrame) -> None:
    mock_return_do_pseudonymize_field(patch_do_pseudonymize_field)
    pseudo_func = PseudoFunction(function_type=PseudoFunctionTypes.REDACT, kwargs=RedactArgs(replacement_string="test"))
    PseudoData.from_pandas(df).on_fields("fnr").with_custom_function(pseudo_func).pseudonymize()

    patch_do_pseudonymize_field.assert_called_once_with(
        path="pseudonymize/field",
        values=df["fnr"].tolist(),
        field_name="fnr",
        pseudo_func=pseudo_func,
        metadata_map={},
        timeout=TIMEOUT_DEFAULT,
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
    pseudo_func = PseudoFunction(function_type=PseudoFunctionTypes.DAEAD, kwargs=DaeadKeywordArgs(key_id="1403797237"))
    keyset = PseudoKeyset(kek_uri=kek_uri, encrypted_keyset=encrypted_keyset, keyset_info=keyset_info)

    PseudoData.from_pandas(df).on_fields("fnr").with_custom_function(pseudo_func).pseudonymize(
        with_custom_keyset=keyset
    )

    patch_do_pseudonymize_field.assert_called_once_with(
        path="pseudonymize/field",
        values=df["fnr"].tolist(),
        field_name="fnr",
        pseudo_func=pseudo_func,
        metadata_map={},
        timeout=TIMEOUT_DEFAULT,
        keyset=keyset,
    )


@patch(f"{PKG}._do_pseudonymize_field")
def test_pseudonymize_field_dataframe_setup(patch_do_pseudonymize_field: MagicMock, df: pd.DataFrame) -> None:
    def side_effect(**kwargs: t.Any) -> pl.Series:
        name = kwargs["field_name"]
        return pl.Series([f"{name}1", f"{name}2", f"{name}3"])

    patch_do_pseudonymize_field.side_effect = side_effect

    fields_to_pseudonymize = "fnr", "fornavn", "etternavn"
    result = PseudoData.from_pandas(df).on_fields(*fields_to_pseudonymize).with_default_encryption().pseudonymize()
    assert isinstance(result, Result)
    dataframe = result.to_pandas()

    for field in fields_to_pseudonymize:
        assert dataframe[field].to_list() == side_effect(field_name=field).to_list()


def test_builder_field_selector_multiple_fields(df: pd.DataFrame) -> None:
    fields = ["snr", "snr_mor", "snr_far"]
    assert PseudoData.from_pandas(df).on_fields(*fields)._fields == [f"{f}" for f in fields]


@pytest.mark.parametrize("supported_mimetype", Mimetypes.__members__.keys())
def test_get_content_type_from_file(supported_mimetype: str) -> None:
    file_extension = supported_mimetype.lower()
    print(file_extension)
    file_handle = open(f"{TEST_FILE_PATH}/test.{file_extension}", mode="rb")
    content_type = _get_content_type_from_file(file_handle)
    assert content_type.name == supported_mimetype


def test_get_content_type_from_file_unsupported_mimetype() -> None:
    file_handle = open(f"{TEST_FILE_PATH}/test.xml", mode="rb")
    with pytest.raises(MimetypeNotSupportedError):
        _get_content_type_from_file(file_handle)


def test_builder_from_file_not_a_file() -> None:
    path = f"{TEST_FILE_PATH}/not/a/file.json"
    with pytest.raises(FileNotFoundError):
        PseudoData.from_file(path)


def test_builder_from_file_no_file_extension() -> None:
    path = f"{TEST_FILE_PATH}/file_no_extension"

    with pytest.raises(NoFileExtensionError):
        PseudoData.from_file(path)


def test_builder_from_file_empty_file() -> None:
    path = f"{TEST_FILE_PATH}/empty_file"

    with pytest.raises(ValueError):
        PseudoData.from_file(path)


@pytest.mark.parametrize("file_format", Mimetypes.__members__.keys())
def test_builder_from_file(file_format: str) -> None:
    # Test reading all supported file extensions
    PseudoData.from_file(f"{TEST_FILE_PATH}/test.{file_format.lower()}")


def test_builder_from_invalid_gcs_file() -> None:
    invalid_gcs_path = "gs://invalid/path.json"
    with pytest.raises(FileNotFoundError):
        PseudoData.from_file(invalid_gcs_path)


@patch(f"{PKG}._do_pseudonymize_field")
def test_builder_to_polars_from_polars_chaining(patch_do_pseudonymize_field: MagicMock, df: pd.DataFrame) -> None:
    def side_effect(**kwargs: t.Any) -> pl.Series:
        name = kwargs["field_name"]
        return pl.Series([f"{name}1", f"{name}2", f"{name}3"])

    patch_do_pseudonymize_field.side_effect = side_effect
    fields_to_pseudonymize = "fnr", "fornavn", "etternavn"
    result: pl.DataFrame = (
        PseudoData.from_pandas(df)
        .on_fields(*fields_to_pseudonymize)
        .with_default_encryption()
        .on_fields("fnr")
        .with_stable_id()
        .pseudonymize()
        .to_polars()
    )
    assert result is not None
