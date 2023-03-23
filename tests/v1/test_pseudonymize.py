"""Test pseudonymize (v1)"""
import io
import json
from unittest import mock

import pandas as pd
import pytest
from typeguard import suppress_type_checks

from dapla_pseudo import pseudonymize
from dapla_pseudo.constants import env
from dapla_pseudo.constants import predefined_keys
from dapla_pseudo.v1.models import Field
from dapla_pseudo.v1.models import Mimetypes
from dapla_pseudo.v1.models import PseudoKeyset


base_url = "https://mocked.dapla-pseudo-service"
auth_token = "some-mocked-token"
custom_keyset = PseudoKeyset.parse_obj(
    {
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
)

REQUESTS_POST = "requests.post"


@pytest.fixture
def test_data_json_file_path() -> str:
    return "tests/data/personer.json"


@mock.patch("dapla.auth.AuthClient")
@mock.patch(REQUESTS_POST)
def test_pseudonymize_with_default_env_values(
    patched_post: mock.Mock, patched_auth_client: mock.Mock, test_data_json_file_path: str
) -> None:
    patched_auth_client.fetch_local_user.return_value = {"access_token": auth_token}

    pseudonymize(test_data_json_file_path, fields=["fnr", "fornavn"])
    patched_auth_client.called_once()
    patched_post.assert_called_once()
    arg = patched_post.call_args.kwargs

    assert arg["url"] == "http://dapla-pseudo-service.dapla.svc.cluster.local/pseudonymize/file"
    assert arg["headers"] == {"Authorization": f"Bearer {auth_token}"}


@mock.patch("dapla.auth.AuthClient")
@mock.patch(REQUESTS_POST)
def test_pseudonymize_dataframe(
    patched_post: mock.Mock, patched_auth_client: mock.Mock, test_data_json_file_path: str
) -> None:
    patched_auth_client.fetch_local_user.return_value = {"access_token": auth_token}
    df = pd.read_json(test_data_json_file_path)

    pseudonymize(df, fields=["fnr", "fornavn"])
    patched_post.assert_called_once()
    arg = patched_post.call_args.kwargs

    assert arg["files"]["data"][0] == "unknown.json"
    assert isinstance(arg["files"]["data"][1], io.BytesIO)
    assert arg["files"]["data"][2] == Mimetypes.JSON


@mock.patch("dapla.auth.AuthClient")
@mock.patch(REQUESTS_POST)
def test_pseudonymize_file_handle(
    patched_post: mock.Mock, patched_auth_client: mock.Mock, test_data_json_file_path: str
) -> None:
    patched_auth_client.fetch_local_user.return_value = {"access_token": auth_token}
    with open(test_data_json_file_path, "rb") as data:
        pseudonymize(data, fields=["fnr", "fornavn"])
    patched_post.assert_called_once()
    arg = patched_post.call_args.kwargs

    assert arg["files"]["data"][0] == "personer.json"
    assert isinstance(arg["files"]["data"][1], io.BufferedReader)
    assert arg["files"]["data"][2] == Mimetypes.JSON


@mock.patch("dapla.auth.AuthClient")
@mock.patch(REQUESTS_POST)
def test_pseudonymize_invalid_type(
    patched_post: mock.Mock, patched_auth_client: mock.Mock, test_data_json_file_path: str
) -> None:
    patched_auth_client.fetch_local_user.return_value = {"access_token": auth_token}

    with open(test_data_json_file_path) as data:
        with suppress_type_checks():
            with pytest.raises(ValueError):
                pseudonymize(data, fields=["fnr", "fornavn"])


@mock.patch(REQUESTS_POST)
def test_pseudonymize_request_with_default_key(
    patched_post: mock.Mock, monkeypatch: pytest.MonkeyPatch, test_data_json_file_path: str
) -> None:
    monkeypatch.setenv(env.PSEUDO_SERVICE_URL, base_url)
    monkeypatch.setenv(env.PSEUDO_SERVICE_AUTH_TOKEN, auth_token)

    pseudonymize(test_data_json_file_path, fields=["fnr", "fornavn"])
    patched_post.assert_called_once()
    arg = patched_post.call_args.kwargs

    assert arg["url"] == f"{base_url}/pseudonymize/file"
    assert arg["headers"] == {"Authorization": f"Bearer {auth_token}"}
    assert arg["stream"] is True

    expected_request_json = json.dumps(
        {
            "pseudoConfig": {
                "rules": [
                    {"name": "rule-1", "pattern": "**/fnr", "func": "daead(keyId=ssb-common-key-1)"},
                    {"name": "rule-2", "pattern": "**/fornavn", "func": "daead(keyId=ssb-common-key-1)"},
                ]
            },
            "targetContentType": "application/json",
        }
    )

    actual_request_json = arg["files"]["request"][1]
    assert actual_request_json == expected_request_json

    assert arg["files"]["data"][0] == "personer.json"
    assert isinstance(arg["files"]["data"][1], io.BufferedReader)
    assert arg["files"]["data"][2] == Mimetypes.JSON


@pytest.mark.parametrize(
    "key", [predefined_keys.SSB_COMMON_KEY_1, predefined_keys.SSB_COMMON_KEY_2, "some-unknown-key"]
)
@mock.patch(REQUESTS_POST)
def test_pseudonymize_request_with_explicitly_specified_common_key(
    patched_post: mock.Mock, monkeypatch: pytest.MonkeyPatch, test_data_json_file_path: str, key: str
) -> None:
    monkeypatch.setenv(env.PSEUDO_SERVICE_URL, base_url)
    monkeypatch.setenv(env.PSEUDO_SERVICE_AUTH_TOKEN, auth_token)

    pseudonymize(test_data_json_file_path, fields=["fnr", "fornavn"], key=key)
    patched_post.assert_called_once()
    arg = patched_post.call_args.kwargs

    assert arg["url"] == f"{base_url}/pseudonymize/file"
    assert arg["headers"] == {"Authorization": f"Bearer {auth_token}"}
    assert arg["stream"] is True

    expected_request_json = json.dumps(
        {
            "pseudoConfig": {
                "rules": [
                    {"name": "rule-1", "pattern": "**/fnr", "func": f"daead(keyId={key})"},
                    {"name": "rule-2", "pattern": "**/fornavn", "func": f"daead(keyId={key})"},
                ]
            },
            "targetContentType": "application/json",
        }
    )
    actual_request_json = arg["files"]["request"][1]
    assert actual_request_json == expected_request_json


@mock.patch(REQUESTS_POST)
def test_pseudonymize_request_with_explicitly_specified_keyset(
    patched_post: mock.Mock, monkeypatch: pytest.MonkeyPatch, test_data_json_file_path: str
) -> None:
    monkeypatch.setenv(env.PSEUDO_SERVICE_URL, base_url)
    monkeypatch.setenv(env.PSEUDO_SERVICE_AUTH_TOKEN, auth_token)

    keyset = custom_keyset

    pseudonymize(test_data_json_file_path, fields=["fnr", "fornavn"], key=keyset)
    patched_post.assert_called_once()
    arg = patched_post.call_args.kwargs

    assert arg["url"] == f"{base_url}/pseudonymize/file"
    assert arg["headers"] == {"Authorization": f"Bearer {auth_token}"}
    assert arg["stream"] is True

    expected_request_json = json.dumps(
        {
            "pseudoConfig": {
                "rules": [
                    {"name": "rule-1", "pattern": "**/fnr", "func": "daead(keyId=1234567890)"},
                    {"name": "rule-2", "pattern": "**/fornavn", "func": "daead(keyId=1234567890)"},
                ],
                "keysets": [
                    {
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
                ],
            },
            "targetContentType": "application/json",
        }
    )
    actual_request_json = arg["files"]["request"][1]
    assert actual_request_json == expected_request_json


@mock.patch(REQUESTS_POST)
def test_pseudonymize_request_with_sid(
    patched_post: mock.Mock, monkeypatch: pytest.MonkeyPatch, test_data_json_file_path: str
) -> None:
    monkeypatch.setenv(env.PSEUDO_SERVICE_URL, base_url)
    monkeypatch.setenv(env.PSEUDO_SERVICE_AUTH_TOKEN, auth_token)

    pseudonymize(
        test_data_json_file_path,
        fields=[Field(pattern="**/fnr", mapping="sid"), {"pattern": "**/fnr2", "mapping": "sid"}, "fornavn"],
    )
    patched_post.assert_called_once()
    arg = patched_post.call_args.kwargs

    assert arg["url"] == f"{base_url}/pseudonymize/file"
    assert arg["headers"] == {"Authorization": f"Bearer {auth_token}"}
    assert arg["stream"] is True

    expected_request_json = json.dumps(
        {
            "pseudoConfig": {
                "rules": [
                    {"name": "rule-1", "pattern": "**/fnr", "func": "map-sid(keyId=papis-common-key-1)"},
                    {"name": "rule-2", "pattern": "**/fnr2", "func": "map-sid(keyId=papis-common-key-1)"},
                    {"name": "rule-3", "pattern": "**/fornavn", "func": "daead(keyId=ssb-common-key-1)"},
                ]
            },
            "targetContentType": "application/json",
        }
    )
    actual_request_json = arg["files"]["request"][1]
    assert actual_request_json == expected_request_json


@mock.patch(REQUESTS_POST)
def test_pseudonymize_sid_fields_only(
    patched_post: mock.Mock, monkeypatch: pytest.MonkeyPatch, test_data_json_file_path: str
) -> None:
    monkeypatch.setenv(env.PSEUDO_SERVICE_URL, base_url)
    monkeypatch.setenv(env.PSEUDO_SERVICE_AUTH_TOKEN, auth_token)

    pseudonymize(
        test_data_json_file_path,
        sid_fields=["fnr"],
    )
    patched_post.assert_called_once()
    arg = patched_post.call_args.kwargs

    assert arg["url"] == f"{base_url}/pseudonymize/file"
    assert arg["headers"] == {"Authorization": f"Bearer {auth_token}"}
    assert arg["stream"] is True

    expected_request_json = json.dumps(
        {
            "pseudoConfig": {
                "rules": [
                    {"name": "rule-1", "pattern": "**/fnr", "func": "map-sid(keyId=papis-common-key-1)"},
                ]
            },
            "targetContentType": "application/json",
        }
    )
    actual_request_json = arg["files"]["request"][1]
    assert actual_request_json == expected_request_json


@mock.patch(REQUESTS_POST)
def test_pseudonymize_request_using_sid_fields_parameter(
    patched_post: mock.Mock, monkeypatch: pytest.MonkeyPatch, test_data_json_file_path: str
) -> None:
    monkeypatch.setenv(env.PSEUDO_SERVICE_URL, base_url)
    monkeypatch.setenv(env.PSEUDO_SERVICE_AUTH_TOKEN, auth_token)

    pseudonymize(test_data_json_file_path, fields=["fornavn"], sid_fields=["fnr"])
    patched_post.assert_called_once()
    arg = patched_post.call_args.kwargs

    assert arg["url"] == f"{base_url}/pseudonymize/file"
    assert arg["headers"] == {"Authorization": f"Bearer {auth_token}"}
    assert arg["stream"] is True

    expected_request_json = json.dumps(
        {
            "pseudoConfig": {
                "rules": [
                    {"name": "rule-1", "pattern": "**/fnr", "func": "map-sid(keyId=papis-common-key-1)"},
                    {"name": "rule-2", "pattern": "**/fornavn", "func": "daead(keyId=ssb-common-key-1)"},
                ]
            },
            "targetContentType": "application/json",
        }
    )
    actual_request_json = arg["files"]["request"][1]
    assert actual_request_json == expected_request_json
