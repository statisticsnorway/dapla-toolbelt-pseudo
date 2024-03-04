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
from dapla_pseudo.v1.api_models import DaeadKeywordArgs
from dapla_pseudo.v1.api_models import DepseudonymizeFileRequest
from dapla_pseudo.v1.api_models import FF31KeywordArgs
from dapla_pseudo.v1.api_models import KeyWrapper
from dapla_pseudo.v1.api_models import MapSidKeywordArgs
from dapla_pseudo.v1.api_models import Mimetypes
from dapla_pseudo.v1.api_models import PseudoConfig
from dapla_pseudo.v1.api_models import PseudoFunction
from dapla_pseudo.v1.api_models import PseudoKeyset
from dapla_pseudo.v1.api_models import PseudoRule
from dapla_pseudo.v1.api_models import RedactArgs
from dapla_pseudo.v1.client import PseudoClient
from dapla_pseudo.v1.depseudo import Depseudonymize
from dapla_pseudo.v1.pseudo_commons import File
from dapla_pseudo.v1.pseudo_commons import RawPseudoMetadata
from dapla_pseudo.v1.pseudo_commons import pseudonymize_operation_field
from dapla_pseudo.v1.result import Result

PKG = "dapla_pseudo.v1.depseudo"
TEST_FILE_PATH = "tests/v1/test_files"


@pytest.fixture()
def pseudonymized_df() -> pd.DataFrame:
    with open("tests/data/personer_pseudonymized.csv") as test_data:
        return pd.read_csv(test_data)


@pytest.fixture()
def pseudonymized_df_polars() -> pl.DataFrame:
    dtypes = {
        "fnr": pl.Utf8,
        "fornavn": pl.Utf8,
        "etternavn": pl.Utf8,
        "kjonn": pl.Categorical,
        "fodselsdato": pl.Utf8,
    }
    return pl.read_csv("tests/data/personer_pseudonymized.csv", dtypes=dtypes)


@pytest.fixture()
def json_pseudonymized_file_path() -> str:
    return "tests/data/personer_pseudonymized.json"


@pytest.fixture()
def json_pseudonymized_hierarch_file_path() -> str:
    return "tests/data/personer_hierarchical_pseudonymized.json"


@pytest.fixture()
def single_field_response() -> MagicMock:
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.content = b'{"data": ["f1","f2","f3"], "datadoc_metadata": {"pseudo_variables": []}, "metrics": [], "logs": []}'
    return mock_response


def mock_return_pseudonymize_operation_field(
    patch_pseudonymize_operation_field: Mock,
) -> None:
    patch_pseudonymize_operation_field.return_value = (
        pl.Series(["e1", "e2", "e3"]),
        RawPseudoMetadata(logs=[], metrics=[], datadoc=[], field_name="tester"),
    )


@patch("dapla_pseudo.v1.PseudoClient._post_to_field_endpoint")
def test_builder_pandas_depseudonymize_minimal_call(
    patched_post_to_field_endpoint: Mock,
    pseudonymized_df: pd.DataFrame,
    single_field_response: MagicMock,
) -> None:
    field_name = "fornavn"

    patched_post_to_field_endpoint.return_value = single_field_response

    pseudo_result = (
        Depseudonymize.from_pandas(pseudonymized_df)
        .on_fields(field_name)
        .with_default_encryption()
        .run()
    )
    assert isinstance(pseudo_result, Result)
    pseudo_dataframe = pseudo_result.to_pandas()

    # TODO: Test for metadata values

    # Check that the pseudonymized df has new values
    assert pseudo_dataframe[field_name].tolist() == ["f1", "f2", "f3"]


@patch("dapla_pseudo.v1.PseudoClient._post_to_field_endpoint")
def test_single_field_pseudonymize_operation_field(
    patched_post_to_field_endpoint: Mock, single_field_response: MagicMock
) -> None:
    patched_post_to_field_endpoint.return_value = single_field_response

    pseudo_func = PseudoFunction(
        function_type=PseudoFunctionTypes.MAP_SID,
        kwargs=MapSidKeywordArgs(key_id="fake-key"),
    )
    data, _ = pseudonymize_operation_field(
        "fake.endpoint",
        "fornavn",
        ["x1", "x2", "x3"],
        pseudo_func,
        TIMEOUT_DEFAULT,
        PseudoClient(pseudo_service_url="mock_url", auth_token="mock_token"),
    )
    assert data.to_list() == ["f1", "f2", "f3"]


def test_depseudo_fields_selector_single_field(pseudonymized_df: pd.DataFrame) -> None:
    Depseudonymize.from_pandas(pseudonymized_df).on_fields("fornavn")._fields = [
        "fornavn"
    ]


def test_builder_fields_selector_single_field_polars(
    pseudonymized_df_polars: pl.DataFrame,
) -> None:
    Depseudonymize.from_polars(pseudonymized_df_polars).on_fields("fornavn")._fields = [
        "fornavn"
    ]


def test_builder_fields_selector_multiple_fields(
    pseudonymized_df: pd.DataFrame,
) -> None:
    Depseudonymize.from_pandas(pseudonymized_df).on_fields("fornavn", "fnr")._fields = [
        "fornavn",
        "fnr",
    ]


@patch(f"{PKG}.pseudo_operation_file")
def test_builder_file_default(
    patched_pseudo_operation_file: MagicMock, json_pseudonymized_file_path: str
) -> None:
    mock_pseudo_file_response = Mock()
    mock_pseudo_file_response.data = File(file_handle=Mock(), content_type=Mock())
    print(type(mock_pseudo_file_response.data))
    patched_pseudo_operation_file.return_value = mock_pseudo_file_response
    Depseudonymize.from_file(json_pseudonymized_file_path).on_fields(
        "fornavn"
    ).with_default_encryption().run()

    depseudonymize_request = DepseudonymizeFileRequest(
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
    file_dataset = t.cast(File, Depseudonymize.dataset)
    patched_pseudo_operation_file.assert_called_once_with(
        file_handle=file_dataset.file_handle,
        pseudo_operation_request=depseudonymize_request,
        input_content_type=Mimetypes.JSON,
    )


@patch(f"{PKG}.pseudo_operation_file")
def test_builder_file_hierarchical(
    patched_pseudo_operation_file: MagicMock, json_pseudonymized_hierarch_file_path: str
) -> None:
    patched_pseudo_operation_file.return_value = Mock()
    Depseudonymize.from_file(json_pseudonymized_hierarch_file_path).on_fields(
        "person_info/fnr"
    ).with_default_encryption().run()

    depseudonymize_request = DepseudonymizeFileRequest(
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
    file_dataset = t.cast(File, Depseudonymize.dataset)
    patched_pseudo_operation_file.assert_called_once_with(
        file_handle=file_dataset.file_handle,
        pseudo_operation_request=depseudonymize_request,
        input_content_type=Mimetypes.JSON,
    )


@patch(f"{PKG}.pseudonymize_operation_field")
def test_builder_pseudo_function_selector_default(
    patch_pseudonymize_operation_field: MagicMock, pseudonymized_df: pd.DataFrame
) -> None:
    mock_return_pseudonymize_operation_field(patch_pseudonymize_operation_field)
    Depseudonymize.from_pandas(pseudonymized_df).on_fields(
        "fornavn"
    ).with_default_encryption().run()
    patch_pseudonymize_operation_field.assert_called_once_with(
        path="depseudonymize/field",
        field_name="fornavn",
        values=pseudonymized_df["fornavn"].tolist(),
        pseudo_func=PseudoFunction(
            function_type=PseudoFunctionTypes.DAEAD, kwargs=DaeadKeywordArgs()
        ),
        timeout=TIMEOUT_DEFAULT,
        pseudo_client=ANY,
        keyset=None,
    )


@patch(f"{PKG}.pseudonymize_operation_field")
def test_builder_pseudo_function_selector_fpe(
    patch_pseudonymize_operation_field: MagicMock, pseudonymized_df: pd.DataFrame
) -> None:
    mock_return_pseudonymize_operation_field(patch_pseudonymize_operation_field)
    Depseudonymize.from_pandas(pseudonymized_df).on_fields(
        "fnr"
    ).with_papis_compatible_encryption().run()
    patch_pseudonymize_operation_field.assert_called_once_with(
        path="depseudonymize/field",
        values=pseudonymized_df["fnr"].tolist(),
        field_name="fnr",
        pseudo_func=PseudoFunction(
            function_type=PseudoFunctionTypes.FF31, kwargs=FF31KeywordArgs()
        ),
        timeout=TIMEOUT_DEFAULT,
        pseudo_client=ANY,
        keyset=None,
    )


@patch(f"{PKG}.pseudonymize_operation_field")
def test_builder_pseudo_function_selector_custom(
    patch_pseudonymize_operation_field: MagicMock, pseudonymized_df: pd.DataFrame
) -> None:
    mock_return_pseudonymize_operation_field(patch_pseudonymize_operation_field)
    pseudo_func = PseudoFunction(
        function_type=PseudoFunctionTypes.FF31, kwargs=FF31KeywordArgs()
    )
    Depseudonymize.from_pandas(pseudonymized_df).on_fields("fnr").with_custom_function(
        pseudo_func
    ).run()

    patch_pseudonymize_operation_field.assert_called_once_with(
        path="depseudonymize/field",
        values=pseudonymized_df["fnr"].tolist(),
        field_name="fnr",
        pseudo_func=pseudo_func,
        timeout=TIMEOUT_DEFAULT,
        pseudo_client=ANY,
        keyset=None,
    )


@patch(f"{PKG}.pseudonymize_operation_field")
def test_builder_pseudo_function_selector_redact(
    patch_pseudonymize_operation_field: MagicMock, pseudonymized_df: pd.DataFrame
) -> None:
    mock_return_pseudonymize_operation_field(patch_pseudonymize_operation_field)
    pseudo_func = PseudoFunction(
        function_type=PseudoFunctionTypes.REDACT,
        kwargs=RedactArgs(replacement_string="test"),
    )
    Depseudonymize.from_pandas(pseudonymized_df).on_fields("fnr").with_custom_function(
        pseudo_func
    ).run()

    patch_pseudonymize_operation_field.assert_called_once_with(
        path="depseudonymize/field",
        values=pseudonymized_df["fnr"].tolist(),
        field_name="fnr",
        pseudo_func=pseudo_func,
        timeout=TIMEOUT_DEFAULT,
        pseudo_client=ANY,
        keyset=None,
    )


@patch(f"{PKG}.pseudonymize_operation_field")
def test_builder_pseudo_keyset_selector_custom(
    patch_pseudonymize_operation_field: MagicMock, pseudonymized_df: pd.DataFrame
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

    Depseudonymize.from_pandas(pseudonymized_df).on_fields("fnr").with_custom_function(
        pseudo_func
    ).run(custom_keyset=keyset)

    patch_pseudonymize_operation_field.assert_called_once_with(
        path="depseudonymize/field",
        values=pseudonymized_df["fnr"].tolist(),
        field_name="fnr",
        pseudo_func=pseudo_func,
        timeout=TIMEOUT_DEFAULT,
        pseudo_client=ANY,
        keyset=keyset,
    )


@patch(f"{PKG}.pseudonymize_operation_field")
def test_pseudonymize_field_dataframe_setup(
    patch_pseudonymize_operation_field: MagicMock, pseudonymized_df: pd.DataFrame
) -> None:
    def side_effect(**kwargs: t.Any) -> tuple[pl.Series, RawPseudoMetadata]:
        name = kwargs["field_name"]
        return (
            pl.Series([f"{name}1", f"{name}2", f"{name}3"]),
            RawPseudoMetadata(logs=[], metrics=[], datadoc=[], field_name="tester"),
        )

    patch_pseudonymize_operation_field.side_effect = side_effect

    fields_to_pseudonymize = "fnr", "fornavn", "etternavn"
    result = (
        Depseudonymize.from_pandas(pseudonymized_df)
        .on_fields(*fields_to_pseudonymize)
        .with_default_encryption()
        .run()
    )
    assert isinstance(result, Result)
    dataframe = result.to_pandas()

    for field in fields_to_pseudonymize:
        assert dataframe[field].to_list() == side_effect(field_name=field)[0].to_list()


def test_builder_field_selector_multiple_fields(pseudonymized_df: pd.DataFrame) -> None:
    fields = ["snr", "snr_mor", "snr_far"]
    assert Depseudonymize.from_pandas(pseudonymized_df).on_fields(*fields)._fields == [
        f"{f}" for f in fields
    ]


def test_builder_from_file_not_a_file() -> None:
    path = f"{TEST_FILE_PATH}/not/a/file.json"
    with pytest.raises(FileNotFoundError):
        Depseudonymize.from_file(path)


def test_builder_from_file_no_file_extension() -> None:
    path = f"{TEST_FILE_PATH}/file_no_extension"

    with pytest.raises(NoFileExtensionError):
        Depseudonymize.from_file(path)


def test_builder_from_file_empty_file() -> None:
    path = f"{TEST_FILE_PATH}/empty_file"

    with pytest.raises(FileInvalidError):
        Depseudonymize.from_file(path)


@pytest.mark.parametrize("file_format", Mimetypes.__members__.keys())
def test_builder_from_file(file_format: str) -> None:
    # Test reading all supported file extensions
    Depseudonymize.from_file(f"{TEST_FILE_PATH}/test.{file_format.lower()}")


def test_builder_from_invalid_gcs_file() -> None:
    invalid_gcs_path = "gs://invalid/path.json"
    with pytest.raises((FileNotFoundError, DefaultCredentialsError)):
        Depseudonymize.from_file(invalid_gcs_path)


@patch(f"{PKG}.pseudonymize_operation_field")
def test_builder_to_polars_from_polars_chaining(
    patch_pseudonymize_operation_field: MagicMock, pseudonymized_df: pd.DataFrame
) -> None:
    def side_effect(**kwargs: t.Any) -> tuple[pl.Series, RawPseudoMetadata]:
        name = kwargs["field_name"]
        return (
            pl.Series([f"{name}1", f"{name}2", f"{name}3"]),
            RawPseudoMetadata(logs=[], metrics=[], datadoc=[], field_name="tester"),
        )

    patch_pseudonymize_operation_field.side_effect = side_effect
    fields_to_depseudonymize = "fnr", "fornavn", "etternavn"
    result: pl.DataFrame = (
        Depseudonymize.from_pandas(pseudonymized_df)
        .on_fields(*fields_to_depseudonymize)
        .with_default_encryption()
        .run()
        .to_polars()
    )
    assert result is not None
