import typing as t
from datetime import date
from unittest.mock import ANY
from unittest.mock import MagicMock
from unittest.mock import Mock
from unittest.mock import patch

import polars as pl
import pytest
from google.auth.exceptions import DefaultCredentialsError

from dapla_pseudo.constants import TIMEOUT_DEFAULT
from dapla_pseudo.constants import PseudoFunctionTypes
from dapla_pseudo.exceptions import FileInvalidError
from dapla_pseudo.exceptions import NoFileExtensionError
from dapla_pseudo.utils import convert_to_date
from dapla_pseudo.v1.api_models import DaeadKeywordArgs
from dapla_pseudo.v1.api_models import FF31KeywordArgs
from dapla_pseudo.v1.api_models import KeyWrapper
from dapla_pseudo.v1.api_models import MapSidKeywordArgs
from dapla_pseudo.v1.api_models import Mimetypes
from dapla_pseudo.v1.api_models import PseudoConfig
from dapla_pseudo.v1.api_models import PseudoFieldRequest
from dapla_pseudo.v1.api_models import PseudoFunction
from dapla_pseudo.v1.api_models import PseudoKeyset
from dapla_pseudo.v1.api_models import PseudonymizeFileRequest
from dapla_pseudo.v1.api_models import PseudoRule
from dapla_pseudo.v1.api_models import RedactArgs
from dapla_pseudo.v1.client import PseudoClient
from dapla_pseudo.v1.pseudo import Pseudonymize
from dapla_pseudo.v1.pseudo_commons import File
from dapla_pseudo.v1.pseudo_commons import RawPseudoMetadata
from dapla_pseudo.v1.pseudo_commons import pseudonymize_operation_field
from dapla_pseudo.v1.result import Result
from tests.v1.test_client import test_client  # noqa:F401

PKG = "dapla_pseudo.v1.pseudo"
TEST_FILE_PATH = "tests/v1/test_files"


def mock_return_pseudonymize_operation_field(
    patch_pseudonymize_operation_field: Mock,
) -> None:
    patch_pseudonymize_operation_field.return_value = (
        pl.Series(["e1", "e2", "e3"]),
        RawPseudoMetadata(logs=[], metrics=[], datadoc=[], field_name="tester"),
    )


@patch("dapla_pseudo.v1.PseudoClient._post_to_field_endpoint")
def test_builder_pandas_pseudonymize_minimal_call(
    patched_post_to_field_endpoint: Mock,
    df_personer: pl.DataFrame,
    single_field_response: MagicMock,
) -> None:
    field_name = "fornavn"

    patched_post_to_field_endpoint.return_value = single_field_response

    pseudo_result = (
        Pseudonymize.from_polars(df_personer)
        .on_fields(field_name)
        .with_default_encryption()
        .run()
    )
    assert isinstance(pseudo_result, Result)
    pseudo_dataframe = pseudo_result.to_pandas()

    # Check that the pseudonymized df has new values
    assert pseudo_dataframe[field_name].tolist() == ["Donald", "Mikke", "Anton"]


@patch("dapla_pseudo.v1.PseudoClient._post_to_field_endpoint")
def test_single_field_do_pseudonymize_field(
    patched_post_to_field_endpoint: Mock,
    single_field_response: MagicMock,
    test_client: PseudoClient,  # noqa: F811
) -> None:
    patched_post_to_field_endpoint.return_value = single_field_response

    pseudo_func = PseudoFunction(
        function_type=PseudoFunctionTypes.MAP_SID,
        kwargs=MapSidKeywordArgs(key_id="fake-key"),
    )
    req = PseudoFieldRequest(
        pseudo_func=pseudo_func, name="fornavn", values=["x1", "x2", "x3"]
    )
    series, _ = pseudonymize_operation_field(
        "fake.endpoint",
        req,
        TIMEOUT_DEFAULT,
        test_client,
    )
    assert series.to_list() == ["Donald", "Mikke", "Anton"]


def test_builder_fields_selector_single_field(df_personer: pl.DataFrame) -> None:
    assert Pseudonymize.from_polars(df_personer).on_fields("fornavn")._fields == [
        "fornavn"
    ]


def test_builder_fields_selector_single_field_polars(df_personer: pl.DataFrame) -> None:
    Pseudonymize.from_polars(df_personer).on_fields("fornavn")._fields = ["fornavn"]


def test_builder_fields_selector_multiple_fields(df_personer: pl.DataFrame) -> None:
    assert Pseudonymize.from_polars(df_personer).on_fields(
        "fornavn", "fnr"
    )._fields == [
        "fornavn",
        "fnr",
    ]


@patch(f"{PKG}.pseudo_operation_file")
def test_builder_file_default(
    patched_pseudo_operation_file: MagicMock, personer_file_path: str
) -> None:
    patched_pseudo_operation_file.return_value = Mock()
    Pseudonymize.from_file(personer_file_path).on_fields(
        "fornavn"
    ).with_default_encryption().run()

    pseudonymize_request = PseudonymizeFileRequest(
        pseudo_config=PseudoConfig(
            rules=[
                PseudoRule(
                    name=None,
                    pattern="**/fornavn",
                    func=PseudoFunction(
                        function_type=PseudoFunctionTypes.DAEAD,
                        kwargs=DaeadKeywordArgs(),
                    ),
                )
            ],
            keysets=KeyWrapper(None).keyset_list(),
        ),
        target_content_type=Mimetypes.JSON,
        target_uri=None,
        compression=None,
    )
    file_dataset = t.cast(File, Pseudonymize.dataset)
    patched_pseudo_operation_file.assert_called_once_with(
        file_handle=file_dataset.file_handle,
        pseudo_operation_request=pseudonymize_request,
        input_content_type=Mimetypes.JSON,
    )


@patch(f"{PKG}.pseudo_operation_file")
def test_builder_file_hierarchical(
    patched_pseudonymize_file: MagicMock, personer_hierarch_file_path: str
) -> None:
    patched_pseudonymize_file.return_value = Mock()
    Pseudonymize.from_file(personer_hierarch_file_path).on_fields(
        "person_info/fnr"
    ).with_default_encryption().run()

    pseudonymize_request = PseudonymizeFileRequest(
        pseudo_config=PseudoConfig(
            rules=[
                PseudoRule(
                    name=None,
                    pattern="**/person_info/fnr",
                    func=PseudoFunction(
                        function_type=PseudoFunctionTypes.DAEAD,
                        kwargs=DaeadKeywordArgs(),
                    ),
                )
            ],
            keysets=KeyWrapper(None).keyset_list(),
        ),
        target_content_type=Mimetypes.JSON,
        target_uri=None,
        compression=None,
    )
    file_dataset = t.cast(File, Pseudonymize.dataset)
    patched_pseudonymize_file.assert_called_once_with(
        file_handle=file_dataset.file_handle,
        pseudo_operation_request=pseudonymize_request,
        input_content_type=Mimetypes.JSON,
    )


@patch(f"{PKG}.pseudonymize_operation_field")
def test_builder_pseudo_function_selector_default(
    patch_pseudonymize_operation_field: MagicMock,
    df_personer: pl.DataFrame,
) -> None:
    mock_return_pseudonymize_operation_field(patch_pseudonymize_operation_field)
    Pseudonymize.from_polars(df_personer).on_fields(
        "fornavn"
    ).with_default_encryption().run()
    pseudo_func = PseudoFunction(
        function_type=PseudoFunctionTypes.DAEAD, kwargs=DaeadKeywordArgs()
    )
    req = PseudoFieldRequest(
        pseudo_func=pseudo_func, name="fornavn", values=df_personer["fornavn"].to_list()
    )
    patch_pseudonymize_operation_field.assert_called_once_with(
        path="pseudonymize/field",
        pseudo_field_request=req,
        timeout=TIMEOUT_DEFAULT,
        pseudo_client=ANY,
    )


@patch(f"{PKG}.pseudonymize_operation_field")
def test_builder_pseudo_function_selector_with_sid(
    patch_pseudonymize_operation_field: MagicMock, df_personer: pl.DataFrame
) -> None:
    mock_return_pseudonymize_operation_field(patch_pseudonymize_operation_field)
    Pseudonymize.from_polars(df_personer).on_fields("fnr").with_stable_id().run()
    pseudo_func = PseudoFunction(
        function_type=PseudoFunctionTypes.MAP_SID, kwargs=MapSidKeywordArgs()
    )
    req = PseudoFieldRequest(
        pseudo_func=pseudo_func, name="fnr", values=df_personer["fnr"].to_list()
    )
    patch_pseudonymize_operation_field.assert_called_once_with(
        path="pseudonymize/field",
        pseudo_field_request=req,
        timeout=TIMEOUT_DEFAULT,
        pseudo_client=ANY,
    )


@patch(f"{PKG}.pseudonymize_operation_field")
def test_builder_pseudo_function_with_sid_snapshot_date_string(
    patch_pseudonymize_operation_field: MagicMock, df_personer: pl.DataFrame
) -> None:
    mock_return_pseudonymize_operation_field(patch_pseudonymize_operation_field)
    Pseudonymize.from_polars(df_personer).on_fields("fnr").with_stable_id(
        sid_snapshot_date=convert_to_date("2023-05-21")
    ).run()
    pseudo_func = PseudoFunction(
        function_type=PseudoFunctionTypes.MAP_SID,
        kwargs=MapSidKeywordArgs(snapshot_date=convert_to_date("2023-05-21")),
    )
    req = PseudoFieldRequest(
        pseudo_func=pseudo_func, name="fnr", values=df_personer["fnr"].to_list()
    )

    patch_pseudonymize_operation_field.assert_called_once_with(
        path="pseudonymize/field",
        pseudo_field_request=req,
        timeout=TIMEOUT_DEFAULT,
        pseudo_client=ANY,
    )


@patch(f"{PKG}.pseudonymize_operation_field")
def test_builder_pseudo_function_with_sid_snapshot_date_date(
    patch_pseudonymize_operation_field: MagicMock, df_personer: pl.DataFrame
) -> None:
    mock_return_pseudonymize_operation_field(patch_pseudonymize_operation_field)
    Pseudonymize.from_polars(df_personer).on_fields("fnr").with_stable_id(
        sid_snapshot_date=date.fromisoformat("2023-05-21")
    ).run()
    pseudo_func = PseudoFunction(
        function_type=PseudoFunctionTypes.MAP_SID,
        kwargs=MapSidKeywordArgs(snapshot_date=date.fromisoformat("2023-05-21")),
    )
    req = PseudoFieldRequest(
        pseudo_func=pseudo_func, name="fnr", values=df_personer["fnr"].to_list()
    )

    patch_pseudonymize_operation_field.assert_called_once_with(
        path="pseudonymize/field",
        pseudo_field_request=req,
        timeout=TIMEOUT_DEFAULT,
        pseudo_client=ANY,
    )


@patch(f"{PKG}.pseudonymize_operation_field")
def test_builder_pseudo_function_selector_fpe(
    patch_pseudonymize_operation_field: MagicMock, df_personer: pl.DataFrame
) -> None:
    mock_return_pseudonymize_operation_field(patch_pseudonymize_operation_field)
    Pseudonymize.from_polars(df_personer).on_fields(
        "fnr"
    ).with_papis_compatible_encryption().run()
    pseudo_func = PseudoFunction(
        function_type=PseudoFunctionTypes.FF31, kwargs=FF31KeywordArgs()
    )
    req = PseudoFieldRequest(
        pseudo_func=pseudo_func, name="fnr", values=df_personer["fnr"].to_list()
    )
    patch_pseudonymize_operation_field.assert_called_once_with(
        path="pseudonymize/field",
        pseudo_field_request=req,
        timeout=TIMEOUT_DEFAULT,
        pseudo_client=ANY,
    )


@patch(f"{PKG}.pseudonymize_operation_field")
def test_builder_pseudo_function_selector_custom(
    patch_pseudonymize_operation_field: MagicMock, df_personer: pl.DataFrame
) -> None:
    mock_return_pseudonymize_operation_field(patch_pseudonymize_operation_field)
    pseudo_func = PseudoFunction(
        function_type=PseudoFunctionTypes.FF31, kwargs=FF31KeywordArgs()
    )
    Pseudonymize.from_polars(df_personer).on_fields("fnr").with_custom_function(
        pseudo_func
    ).run()
    req = PseudoFieldRequest(
        pseudo_func=pseudo_func, name="fnr", values=df_personer["fnr"].to_list()
    )

    patch_pseudonymize_operation_field.assert_called_once_with(
        path="pseudonymize/field",
        pseudo_field_request=req,
        timeout=TIMEOUT_DEFAULT,
        pseudo_client=ANY,
    )


@patch(f"{PKG}.pseudonymize_operation_field")
def test_builder_pseudo_function_selector_redact(
    patch_pseudonymize_operation_field: MagicMock, df_personer: pl.DataFrame
) -> None:
    mock_return_pseudonymize_operation_field(patch_pseudonymize_operation_field)
    pseudo_func = PseudoFunction(
        function_type=PseudoFunctionTypes.REDACT,
        kwargs=RedactArgs(replacement_string="test"),
    )
    Pseudonymize.from_polars(df_personer).on_fields("fnr").with_custom_function(
        pseudo_func
    ).run()

    req = PseudoFieldRequest(
        pseudo_func=pseudo_func, name="fnr", values=df_personer["fnr"].to_list()
    )

    patch_pseudonymize_operation_field.assert_called_once_with(
        path="pseudonymize/field",
        pseudo_field_request=req,
        timeout=TIMEOUT_DEFAULT,
        pseudo_client=ANY,
    )


@patch(f"{PKG}.pseudonymize_operation_field")
def test_builder_pseudo_keyset_selector_custom(
    patch_pseudonymize_operation_field: MagicMock, df_personer: pl.DataFrame
) -> None:
    mock_return_pseudonymize_operation_field(patch_pseudonymize_operation_field)

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
        kwargs=DaeadKeywordArgs(key_id="123"),
    )
    keyset = PseudoKeyset(
        kek_uri=kek_uri, encrypted_keyset=encrypted_keyset, keyset_info=keyset_info
    )

    Pseudonymize.from_polars(df_personer).on_fields("fnr").with_custom_function(
        pseudo_func
    ).run(custom_keyset=keyset)

    req = PseudoFieldRequest(
        pseudo_func=pseudo_func,
        name="fnr",
        values=df_personer["fnr"].to_list(),
        keyset=keyset,
    )

    patch_pseudonymize_operation_field.assert_called_once_with(
        path="pseudonymize/field",
        pseudo_field_request=req,
        timeout=TIMEOUT_DEFAULT,
        pseudo_client=ANY,
    )


@patch(f"{PKG}.pseudonymize_operation_field")
def test_pseudonymize_field_dataframe_setup(
    patch_pseudonymize_operation_field: MagicMock, df_personer: pl.DataFrame
) -> None:
    def side_effect(**kwargs: t.Any) -> tuple[pl.Series, RawPseudoMetadata]:
        name = kwargs["pseudo_field_request"].name
        return pl.Series([f"{name}1", f"{name}2", f"{name}3"]), RawPseudoMetadata(
            logs=[], metrics=[], datadoc=[], field_name="tester"
        )

    patch_pseudonymize_operation_field.side_effect = side_effect

    fields_to_pseudonymize = "fnr", "fornavn", "etternavn"
    result = (
        Pseudonymize.from_polars(df_personer)
        .on_fields(*fields_to_pseudonymize)
        .with_default_encryption()
        .run()
    )
    assert isinstance(result, Result)
    dataframe = result.to_pandas()

    for field in fields_to_pseudonymize:
        func = PseudoFunction(
            function_type=PseudoFunctionTypes.DAEAD, kwargs=DaeadKeywordArgs()
        )
        req = PseudoFieldRequest(pseudo_func=func, name=field, values=[])
        assert (
            dataframe[field].to_list()
            == side_effect(pseudo_field_request=req)[0].to_list()
        )


def test_builder_field_selector_multiple_fields(df_personer: pl.DataFrame) -> None:
    fields = ["snr", "snr_mor", "snr_far"]
    assert Pseudonymize.from_polars(df_personer).on_fields(*fields)._fields == [
        f"{f}" for f in fields
    ]


def test_builder_from_file_not_a_file() -> None:
    path = f"{TEST_FILE_PATH}/not/a/file.json"
    with pytest.raises(FileNotFoundError):
        Pseudonymize.from_file(path)


def test_builder_from_file_no_file_extension() -> None:
    path = f"{TEST_FILE_PATH}/file_no_extension"

    with pytest.raises(NoFileExtensionError):
        Pseudonymize.from_file(path)


def test_builder_from_file_empty_file() -> None:
    path = f"{TEST_FILE_PATH}/empty_file"

    with pytest.raises(FileInvalidError):
        Pseudonymize.from_file(path)


@pytest.mark.parametrize("file_format", Mimetypes.__members__.keys())
def test_builder_from_file(file_format: str) -> None:
    # Test reading all supported file extensions
    Pseudonymize.from_file(f"{TEST_FILE_PATH}/test.{file_format.lower()}")


def test_builder_from_invalid_gcs_file() -> None:
    invalid_gcs_path = "gs://invalid/path.json"
    with pytest.raises((FileNotFoundError, DefaultCredentialsError)):
        Pseudonymize.from_file(invalid_gcs_path)


@patch(f"{PKG}.pseudonymize_operation_field")
def test_builder_to_polars_from_polars_chaining(
    patch_pseudonymize_operation_field: MagicMock, df_personer: pl.DataFrame
) -> None:
    def side_effect(**kwargs: t.Any) -> tuple[pl.Series, RawPseudoMetadata]:
        name = kwargs["pseudo_field_request"].name
        return pl.Series([f"{name}1", f"{name}2", f"{name}3"]), RawPseudoMetadata(
            logs=[], metrics=[], datadoc=[], field_name="tester"
        )

    patch_pseudonymize_operation_field.side_effect = side_effect
    fields_to_pseudonymize = "fnr", "fornavn", "etternavn"
    result: pl.DataFrame = (
        Pseudonymize.from_polars(df_personer)
        .on_fields(*fields_to_pseudonymize)
        .with_default_encryption()
        .on_fields("fnr")
        .with_stable_id()
        .run()
        .to_polars()
    )
    assert result is not None
