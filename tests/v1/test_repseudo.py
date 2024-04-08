import typing as t
from unittest.mock import ANY
from unittest.mock import MagicMock
from unittest.mock import Mock
from unittest.mock import patch

import pandas as pd
import polars as pl
import pytest
from google.auth.exceptions import DefaultCredentialsError

from dapla_pseudo.constants import TIMEOUT_DEFAULT
from dapla_pseudo.constants import PseudoFunctionTypes
from dapla_pseudo.exceptions import FileInvalidError
from dapla_pseudo.exceptions import NoFileExtensionError
from dapla_pseudo.v1.models.api import RawPseudoMetadata
from dapla_pseudo.v1.models.api import RepseudoFieldRequest
from dapla_pseudo.v1.models.api import RepseudoFileRequest
from dapla_pseudo.v1.models.core import DaeadKeywordArgs
from dapla_pseudo.v1.models.core import FF31KeywordArgs
from dapla_pseudo.v1.models.core import File
from dapla_pseudo.v1.models.core import KeyWrapper
from dapla_pseudo.v1.models.core import MapSidKeywordArgs
from dapla_pseudo.v1.models.core import Mimetypes
from dapla_pseudo.v1.models.core import PseudoConfig
from dapla_pseudo.v1.models.core import PseudoFunction
from dapla_pseudo.v1.models.core import PseudoKeyset
from dapla_pseudo.v1.models.core import PseudoRule
from dapla_pseudo.v1.repseudo import Repseudonymize
from dapla_pseudo.v1.result import Result

PKG = "dapla_pseudo.v1.repseudo"
TEST_FILE_PATH = "tests/v1/test_files"


def mock_return_pseudonymize_operation_field(
    patch_pseudonymize_operation_field: Mock,
) -> None:
    patch_pseudonymize_operation_field.return_value = (
        pl.Series(["e1", "e2", "e3"]),
        RawPseudoMetadata(logs=[], metrics=[], datadoc=[], field_name="tester"),
    )


@patch("dapla_pseudo.v1.PseudoClient._post_to_field_endpoint")
def test_builder_repseudonymize_minimal_call(
    patched_post_to_field_endpoint: Mock,
    df_personer_fnr_daead_encrypted: pl.DataFrame,
    single_field_response: MagicMock,
) -> None:
    field_name = "fornavn"

    patched_post_to_field_endpoint.return_value = single_field_response

    pseudo_result = (
        Repseudonymize.from_polars(df_personer_fnr_daead_encrypted)
        .on_fields(field_name)
        .from_default_encryption()
        .to_default_encryption()
        .run()
    )
    assert isinstance(pseudo_result, Result)
    pseudo_dataframe = pseudo_result.to_pandas()

    # TODO: Test for metadata values
    # Check that the pseudonymized df has new values
    assert pseudo_dataframe[field_name].tolist() == ["Donald", "Mikke", "Anton"]


"""
@patch("dapla_pseudo.v1.PseudoClient._post_to_field_endpoint")
def test_single_field_pseudonymize_operation_field(
    patched_post_to_field_endpoint: Mock, single_field_response: MagicMock
) -> None:
    patched_post_to_field_endpoint.return_value = single_field_response

    source_pseudo_func = PseudoFunction(
        function_type=PseudoFunctionTypes.MAP_SID,
        kwargs=MapSidKeywordArgs(key_id="fake-key"),
    )
    target_pseudo_func = PseudoFunction(
        function_type=PseudoFunctionTypes.FF31,
        kwargs=FF31KeywordArgs(key_id="fake-key"),
    )
    req = RepseudoFieldRequest(
        source_pseudo_func=source_pseudo_func,
        target_pseudo_func=target_pseudo_func,
        name="fornavn",
        values=["1", "2", "3"],
    )
    data, _ = pseudonymize_operation_field(
        "fake.endpoint",
        req,
        TIMEOUT_DEFAULT,
        PseudoClient(pseudo_service_url="mock_url", auth_token="mock_token"),
    )
    assert data.to_list() == ["Donald", "Mikke", "Anton"]
"""


def test_depseudo_fields_selector_single_field(
    df_personer_fnr_daead_encrypted: pl.DataFrame,
) -> None:
    assert Repseudonymize.from_polars(df_personer_fnr_daead_encrypted).on_fields(
        "fornavn"
    )._fields == ["fornavn"]


def test_builder_fields_selector_single_field_pandas(
    df_pandas_personer_fnr_daead_encrypted: pd.DataFrame,
) -> None:
    assert Repseudonymize.from_pandas(df_pandas_personer_fnr_daead_encrypted).on_fields(
        "fornavn"
    )._fields == ["fornavn"]


def test_builder_fields_selector_multiple_fields(
    df_personer_fnr_daead_encrypted: pl.DataFrame,
) -> None:
    assert Repseudonymize.from_polars(df_personer_fnr_daead_encrypted).on_fields(
        "fornavn", "fnr"
    )._fields == [
        "fornavn",
        "fnr",
    ]


@patch(f"{PKG}.pseudonymize_operation_field")
def test_builder_repseudo_function_selector_with_sid(
    patch_pseudonymize_operation_field: MagicMock, df_personer: pl.DataFrame
) -> None:
    mock_return_pseudonymize_operation_field(patch_pseudonymize_operation_field)
    Repseudonymize.from_polars(df_personer).on_fields(
        "fnr"
    ).from_default_encryption().to_stable_id().run()
    source_pseudo_func = PseudoFunction(
        function_type=PseudoFunctionTypes.DAEAD, kwargs=DaeadKeywordArgs()
    )
    target_pseudo_func = PseudoFunction(
        function_type=PseudoFunctionTypes.MAP_SID, kwargs=MapSidKeywordArgs()
    )
    req = RepseudoFieldRequest(
        source_pseudo_func=source_pseudo_func,
        target_pseudo_func=target_pseudo_func,
        name="fnr",
        values=df_personer["fnr"].to_list(),
    )
    patch_pseudonymize_operation_field.assert_called_once_with(
        path="repseudonymize/field",
        pseudo_field_request=req,
        timeout=TIMEOUT_DEFAULT,
        pseudo_client=ANY,
    )


@patch(f"{PKG}.pseudo_operation_dataset")
def test_builder_file_default(
    patched_pseudo_operation_dataset: MagicMock, personer_pseudonymized_file_path: str
) -> None:
    mock_pseudo_file_response = Mock()
    mock_pseudo_file_response.data = File(file_handle=Mock(), content_type=Mock())
    patched_pseudo_operation_dataset.return_value = mock_pseudo_file_response

    Repseudonymize.from_file(personer_pseudonymized_file_path).on_fields(
        "fornavn"
    ).from_default_encryption().to_default_encryption().run()

    pseudo_config = PseudoConfig(
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
    )
    repseudonymize_request = RepseudoFileRequest(
        source_pseudo_config=pseudo_config,
        target_pseudo_config=pseudo_config,  # the same config used
        target_content_type=Mimetypes.JSON,
        target_uri=None,
        compression=None,
    )
    file_dataset = t.cast(File, Repseudonymize.dataset)
    patched_pseudo_operation_dataset.assert_called_once_with(
        dataset_ref=file_dataset,
        pseudo_operation_request=repseudonymize_request,
    )


@patch(f"{PKG}.pseudo_operation_dataset")
def test_builder_file_hierarchical(
    patched_pseudo_operation_dataset: MagicMock,
    personer_pseudonymized_hierarch_file_path: str,
) -> None:
    patched_pseudo_operation_dataset.return_value = Mock()
    Repseudonymize.from_file(personer_pseudonymized_hierarch_file_path).on_fields(
        "person_info/fnr"
    ).from_default_encryption().to_default_encryption().run()
    pseudo_config = PseudoConfig(
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
    )
    repseudonymize_request = RepseudoFileRequest(
        source_pseudo_config=pseudo_config,
        target_pseudo_config=pseudo_config,
        target_content_type=Mimetypes.JSON,
        target_uri=None,
        compression=None,
    )
    file_dataset = t.cast(File, Repseudonymize.dataset)
    patched_pseudo_operation_dataset.assert_called_once_with(
        dataset_ref=file_dataset,
        pseudo_operation_request=repseudonymize_request,
    )


@patch(f"{PKG}.pseudonymize_operation_field")
def test_builder_pseudo_function_selector_custom(
    patch_pseudonymize_operation_field: MagicMock, df_personer: pl.DataFrame
) -> None:
    mock_return_pseudonymize_operation_field(patch_pseudonymize_operation_field)
    pseudo_func = PseudoFunction(
        function_type=PseudoFunctionTypes.FF31, kwargs=FF31KeywordArgs()
    )
    Repseudonymize.from_polars(df_personer).on_fields("fnr").from_custom_function(
        pseudo_func
    ).to_custom_function(pseudo_func).run()

    req = RepseudoFieldRequest(
        source_pseudo_func=pseudo_func,
        target_pseudo_func=pseudo_func,
        name="fnr",
        values=df_personer["fnr"].to_list(),
    )

    patch_pseudonymize_operation_field.assert_called_once_with(
        path="repseudonymize/field",
        pseudo_field_request=req,
        timeout=TIMEOUT_DEFAULT,
        pseudo_client=ANY,
    )


@patch(f"{PKG}.pseudonymize_operation_field")
def test_builder_repseudo_keyset_differing_key(
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
    source_pseudo_func = PseudoFunction(
        function_type=PseudoFunctionTypes.DAEAD,
        kwargs=DaeadKeywordArgs(),
    )
    target_pseudo_func = PseudoFunction(
        function_type=PseudoFunctionTypes.DAEAD,
        kwargs=DaeadKeywordArgs(key_id="123"),
    )
    keyset = PseudoKeyset(
        kek_uri=kek_uri, encrypted_keyset=encrypted_keyset, keyset_info=keyset_info
    )

    Repseudonymize.from_polars(df_personer).on_fields(
        "fnr"
    ).from_default_encryption().to_default_encryption(custom_key="123").run(
        custom_target_keyset=keyset
    )

    req = RepseudoFieldRequest(
        source_pseudo_func=source_pseudo_func,
        target_pseudo_func=target_pseudo_func,
        name="fnr",
        values=df_personer["fnr"].to_list(),
        target_keyset=keyset,
    )

    patch_pseudonymize_operation_field.assert_called_once_with(
        path="repseudonymize/field",
        pseudo_field_request=req,
        timeout=TIMEOUT_DEFAULT,
        pseudo_client=ANY,
    )


def test_builder_field_selector_multiple_fields(df_personer: pl.DataFrame) -> None:
    fields = ["snr", "snr_mor", "snr_far"]
    assert Repseudonymize.from_polars(df_personer).on_fields(*fields)._fields == [
        f"{f}" for f in fields
    ]


def test_builder_from_file_not_a_file() -> None:
    path = f"{TEST_FILE_PATH}/not/a/file.json"
    with pytest.raises(FileNotFoundError):
        Repseudonymize.from_file(path)


def test_builder_from_file_no_file_extension() -> None:
    path = f"{TEST_FILE_PATH}/file_no_extension"

    with pytest.raises(NoFileExtensionError):
        Repseudonymize.from_file(path)


def test_builder_from_file_empty_file() -> None:
    path = f"{TEST_FILE_PATH}/empty_file"

    with pytest.raises(FileInvalidError):
        Repseudonymize.from_file(path)


@pytest.mark.parametrize("file_format", Mimetypes.__members__.keys())
def test_builder_from_file(file_format: str) -> None:
    # Test reading all supported file extensions
    Repseudonymize.from_file(f"{TEST_FILE_PATH}/test.{file_format.lower()}")


def test_builder_from_invalid_gcs_file() -> None:
    invalid_gcs_path = "gs://invalid/path.json"
    with pytest.raises((FileNotFoundError, DefaultCredentialsError)):
        Repseudonymize.from_file(invalid_gcs_path)


@patch(f"{PKG}.pseudonymize_operation_field")
def test_builder_to_polars_from_polars_chaining(
    patch_pseudonymize_operation_field: MagicMock, df_personer: pl.DataFrame
) -> None:
    def side_effect(**kwargs: t.Any) -> tuple[pl.Series, RawPseudoMetadata]:
        name = kwargs["pseudo_field_request"]
        return (
            pl.Series([f"{name}1", f"{name}2", f"{name}3"]),
            RawPseudoMetadata(logs=[], metrics=[], datadoc=[], field_name="tester"),
        )

    patch_pseudonymize_operation_field.side_effect = side_effect
    fields_to_depseudonymize = "fnr", "fornavn", "etternavn"
    result: pl.DataFrame = (
        Repseudonymize.from_polars(df_personer)
        .on_fields(*fields_to_depseudonymize)
        .from_default_encryption()
        .to_default_encryption()
        .run()
        .to_polars()
    )
    assert result is not None
