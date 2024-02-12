import json
from collections.abc import Iterator
from io import BufferedReader
from itertools import product
from pathlib import Path
from unittest.mock import Mock

import pandas as pd
import polars as pl
import pytest
from requests import Response
from typeguard import suppress_type_checks

from dapla_pseudo.constants import PseudoFunctionTypes
from dapla_pseudo.v1.api_models import DaeadKeywordArgs
from dapla_pseudo.v1.api_models import FF31KeywordArgs
from dapla_pseudo.v1.api_models import KeyWrapper
from dapla_pseudo.v1.api_models import Mimetypes
from dapla_pseudo.v1.api_models import PseudoFunction
from dapla_pseudo.v1.api_models import PseudoKeyset
from dapla_pseudo.v1.api_models import RedactArgs
from dapla_pseudo.v1.pseudo_commons import PseudoFieldResponse
from dapla_pseudo.v1.pseudo_commons import PseudoFileResponse
from dapla_pseudo.v1.result import Result

TEST_FILE_PATH = "tests/v1/test_files"


@pytest.fixture()
def polars_df() -> pl.DataFrame:
    with open("tests/data/personer.json") as test_data:
        return pl.from_pandas(pd.json_normalize(json.load(test_data)))


@pytest.fixture(
    params=list(product([True, False], Mimetypes.__members__.values()))
)  # tabulate every combination of "streamed" and "content_type"
def pseudo_file_response(request: pytest.FixtureRequest) -> PseudoFileResponse:
    def get_byte_iterator(file_handle: BufferedReader) -> Iterator[bytes]:
        while True:
            chunk = file_handle.read(128)
            if not chunk:  # Last chunk = empty string
                break
            yield chunk

    streamed, content_type = request.param

    fd = open(f"{TEST_FILE_PATH}/test.{content_type.split('/')[-1]}", "rb")
    content = fd.read()
    fd.seek(0)
    content_iterator = get_byte_iterator(fd)

    response_mock = Mock(spec=Response)
    response_mock.content = content
    response_mock.iter_content.return_value = content_iterator
    response_mock.text = content.decode("utf-8")

    return PseudoFileResponse(response_mock, content_type, streamed)


def test_result_from_polars_to_polars(polars_df: pl.DataFrame) -> None:
    result = Result(PseudoFieldResponse(data=polars_df, raw_metadata=[]))
    assert isinstance(result.to_polars(), pl.DataFrame)


def test_result_from_polars_to_pandas(polars_df: pl.DataFrame) -> None:
    result = Result(PseudoFieldResponse(data=polars_df, raw_metadata=[]))
    assert isinstance(result.to_pandas(), pd.DataFrame)


# File tests commented out while we push out metadata
# def test_result_from_polars_to_file(tmp_path: Path, polars_df: pl.DataFrame) -> None:
#    result = Result(PseudoFieldResponse(data=polars_df, raw_metadata=[]))
#    result.to_file(tmp_path / "polars_to_file.json")


def test_result_from_file_to_polars(pseudo_file_response: PseudoFileResponse) -> None:
    result = Result(pseudo_response=pseudo_file_response)
    assert isinstance(result.to_polars(), pl.DataFrame)


def test_result_from_file_to_pandas(pseudo_file_response: PseudoFileResponse) -> None:
    result = Result(pseudo_response=pseudo_file_response)
    assert isinstance(result.to_pandas(), pd.DataFrame)


def test_result_from_file_to_file(
    tmp_path: Path, pseudo_file_response: PseudoFileResponse
) -> None:
    result = Result(pseudo_response=pseudo_file_response)
    file_extension = pseudo_file_response.content_type.name.lower()
    result.to_file(tmp_path / f"file_to_file.{file_extension}")


@suppress_type_checks  # type: ignore [misc]
def test_result_to_pandas_invalid_type() -> None:
    result = Result(pseudo_response="not a DataFrame")  # type: ignore [arg-type]
    with pytest.raises(ValueError):
        result.to_pandas()


@suppress_type_checks  # type: ignore [misc]
def test_result_to_polars_invalid_type() -> None:
    result = Result(pseudo_response="not a DataFrame")  # type: ignore [arg-type]
    with pytest.raises(ValueError):
        result.to_polars()


@suppress_type_checks  # type: ignore [misc]
def test_result_to_file_invalid_type(tmp_path: Path) -> None:
    result = Result(pseudo_response="not a file")  # type: ignore [arg-type]
    with pytest.raises(ValueError):
        result.to_file(tmp_path / "invalid_type.json")


def test_result_different_file_format(tmp_path: Path) -> None:
    response = Mock(spec=Response)
    result = Result(
        pseudo_response=PseudoFileResponse(response, Mimetypes.JSON, streamed=True)
    )
    with pytest.raises(ValueError):
        result.to_file(tmp_path / "invalid_file_format.csv")


custom_keyset_dict = {
    "encryptedKeyset": "CiQAp91NBhLdknX3j9jF6vwhdyURaqcT9/M/iczV7fLn...8XYFKwxiwMtCzDT6QGzCCCM=",
    "keysetInfo": {
        "primaryKeyId": 1234567890,
        "keyInfo": [
            {
                "typeUrl": "type.googleapis.com/google.crypto.tink.AesSivKey",
                "status": "ENABLED",
                "keyId": 1234567890,
                "outputPrefixType": "TINK",
            }
        ],
    },
    "kekUri": "gcp-kms://projects/some-project-id/locations/europe-north1/keyRings/some-keyring/cryptoKeys/some-kek-1",
}


def test_parse_pseudo_keyset() -> None:
    keyset = PseudoKeyset.model_validate(custom_keyset_dict)
    assert keyset.keyset_info["primaryKeyId"] == 1234567890
    assert (
        keyset.encrypted_keyset
        == "CiQAp91NBhLdknX3j9jF6vwhdyURaqcT9/M/iczV7fLn...8XYFKwxiwMtCzDT6QGzCCCM="
    )
    assert (
        keyset.kek_uri
        == "gcp-kms://projects/some-project-id/locations/europe-north1/keyRings/some-keyring/cryptoKeys/some-kek-1"
    )


def test_key_wrapper_with_key_reference() -> None:
    key_wrapper = KeyWrapper("ssb-common-key-1")
    assert key_wrapper.key_id == "ssb-common-key-1"
    assert key_wrapper.keyset is None
    assert key_wrapper.keyset_list() is None


def test_key_wrapper_with_parsed_keyset() -> None:
    keyset = PseudoKeyset.model_validate(custom_keyset_dict)
    key_wrapper = KeyWrapper(key=keyset)
    assert key_wrapper.key_id == "1234567890"
    assert key_wrapper.keyset == keyset


def test_key_wrapper_with_keyset_json() -> None:
    key_wrapper = KeyWrapper(key=json.dumps(custom_keyset_dict))
    assert key_wrapper.key_id == "1234567890"
    assert key_wrapper.keyset == PseudoKeyset.model_validate(custom_keyset_dict)


def test_pseudo_function() -> None:
    assert "daead(keyId=ssb-common-key-1)" == str(
        PseudoFunction(
            function_type=PseudoFunctionTypes.DAEAD, kwargs=DaeadKeywordArgs()
        )
    )


def test_redact_function() -> None:
    assert "redact(test)" == str(
        PseudoFunction(
            function_type=PseudoFunctionTypes.REDACT,
            kwargs=RedactArgs(replacement_string="test"),
        )
    )


def test_pseudo_function_with_extra_kwargs() -> None:
    assert "ff31(keyId=papis-common-key-1,strategy=skip)" == str(
        PseudoFunction(
            function_type=PseudoFunctionTypes.FF31,
            kwargs=FF31KeywordArgs(),
        )
    )
