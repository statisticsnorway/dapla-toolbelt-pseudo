"""Test pseudonymize (v1)"""
import json
from unittest import mock

import pytest

from dapla_pseudo import pseudonymize
from dapla_pseudo.constants import env
from dapla_pseudo.constants import predefined_keys
from dapla_pseudo.utils import find_multipart_obj
from dapla_pseudo.v1.models import Field
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


@mock.patch("dapla.auth.AuthClient")
def test_pseudonymize_with_default_env_values(patched_auth_client: mock.Mock) -> None:
    auth_token = "mocked auth token"
    patched_auth_client.fetch_local_user.return_value = {"access_token": auth_token}

    with mock.patch("requests.post") as patched_post:
        pseudonymize(file_path="tests/data/personer.json", fields=["fnr", "fornavn"])
        patched_auth_client.called_once()
        patched_post.assert_called_once()
        arg = patched_post.call_args.kwargs

        assert arg["url"] == "http://dapla-pseudo-service.dapla.svc.cluster.local/pseudonymize/file"
        assert arg["headers"] == {"Authorization": f"Bearer {auth_token}"}


def test_pseudonymize_request_with_default_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(env.PSEUDO_SERVICE_URL, base_url)
    monkeypatch.setenv(env.PSEUDO_SERVICE_AUTH_TOKEN, auth_token)

    with mock.patch("requests.post") as patched:
        pseudonymize(file_path="tests/data/personer.json", fields=["fnr", "fornavn"])
        patched.assert_called_once()
        arg = patched.call_args.kwargs

        assert arg["url"] == f"{base_url}/pseudonymize/file"
        assert arg["headers"] == {"Authorization": f"Bearer {auth_token}"}
        assert arg["stream"] is True

        expected_request_json = json.dumps(
            {
                "pseudoConfig": {
                    "rules": [
                        {"name": "rule-1", "pattern": "**/fnr", "func": "tink-daead(ssb-common-key-1)"},
                        {"name": "rule-2", "pattern": "**/fornavn", "func": "tink-daead(ssb-common-key-1)"},
                    ]
                },
                "targetContentType": "application/json",
            }
        )
        actual_request_json = find_multipart_obj("request", arg["files"])
        assert actual_request_json == expected_request_json


@pytest.mark.parametrize(
    "key", [predefined_keys.SSB_COMMON_KEY_1, predefined_keys.SSB_COMMON_KEY_2, "some-unknown-key"]
)
def test_pseudonymize_request_with_explicitly_specified_common_key(monkeypatch: pytest.MonkeyPatch, key: str) -> None:
    monkeypatch.setenv(env.PSEUDO_SERVICE_URL, base_url)
    monkeypatch.setenv(env.PSEUDO_SERVICE_AUTH_TOKEN, auth_token)

    with mock.patch("requests.post") as patched:
        pseudonymize(file_path="tests/data/personer.json", fields=["fnr", "fornavn"], key=key)
        patched.assert_called_once()
        arg = patched.call_args.kwargs

        assert arg["url"] == f"{base_url}/pseudonymize/file"
        assert arg["headers"] == {"Authorization": f"Bearer {auth_token}"}
        assert arg["stream"] is True

        expected_request_json = json.dumps(
            {
                "pseudoConfig": {
                    "rules": [
                        {"name": "rule-1", "pattern": "**/fnr", "func": f"tink-daead({key})"},
                        {"name": "rule-2", "pattern": "**/fornavn", "func": f"tink-daead({key})"},
                    ]
                },
                "targetContentType": "application/json",
            }
        )
        actual_request_json = find_multipart_obj("request", arg["files"])
        assert actual_request_json == expected_request_json


def test_pseudonymize_request_with_explicitly_specified_keyset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(env.PSEUDO_SERVICE_URL, base_url)
    monkeypatch.setenv(env.PSEUDO_SERVICE_AUTH_TOKEN, auth_token)

    keyset = custom_keyset

    with mock.patch("requests.post") as patched:
        pseudonymize(file_path="tests/data/personer.json", fields=["fnr", "fornavn"], key=keyset)
        patched.assert_called_once()
        arg = patched.call_args.kwargs

        assert arg["url"] == f"{base_url}/pseudonymize/file"
        assert arg["headers"] == {"Authorization": f"Bearer {auth_token}"}
        assert arg["stream"] is True

        expected_request_json = json.dumps(
            {
                "pseudoConfig": {
                    "rules": [
                        {"name": "rule-1", "pattern": "**/fnr", "func": "tink-daead(1234567890)"},
                        {"name": "rule-2", "pattern": "**/fornavn", "func": "tink-daead(1234567890)"},
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
        actual_request_json = find_multipart_obj("request", arg["files"])
        assert actual_request_json == expected_request_json


def test_pseudonymize_request_with_sid(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(env.PSEUDO_SERVICE_URL, base_url)
    monkeypatch.setenv(env.PSEUDO_SERVICE_AUTH_TOKEN, auth_token)

    with mock.patch("requests.post") as patched:
        pseudonymize(file_path="tests/data/personer.json", fields=[Field(pattern="**/fnr", mapping="sid"), "fornavn"])
        patched.assert_called_once()
        arg = patched.call_args.kwargs

        assert arg["url"] == f"{base_url}/pseudonymize/file"
        assert arg["headers"] == {"Authorization": f"Bearer {auth_token}"}
        assert arg["stream"] is True

        expected_request_json = json.dumps(
            {
                "pseudoConfig": {
                    "rules": [
                        {"name": "rule-1", "pattern": "**/fnr", "func": "map-sid(ssb-common-key-1)"},
                        {"name": "rule-2", "pattern": "**/fornavn", "func": "tink-daead(ssb-common-key-1)"},
                    ]
                },
                "targetContentType": "application/json",
            }
        )
        actual_request_json = find_multipart_obj("request", arg["files"])
        assert actual_request_json == expected_request_json


def test_pseudonymize_request_with_sid2(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(env.PSEUDO_SERVICE_URL, base_url)
    monkeypatch.setenv(env.PSEUDO_SERVICE_AUTH_TOKEN, auth_token)

    with mock.patch("requests.post") as patched:
        pseudonymize(file_path="tests/data/personer.json", fields=["fornavn"], sid=["fnr"])
        patched.assert_called_once()
        arg = patched.call_args.kwargs

        assert arg["url"] == f"{base_url}/pseudonymize/file"
        assert arg["headers"] == {"Authorization": f"Bearer {auth_token}"}
        assert arg["stream"] is True

        expected_request_json = json.dumps(
            {
                "pseudoConfig": {
                    "rules": [
                        {"name": "rule-1", "pattern": "**/fnr", "func": "map-sid(ssb-common-key-1)"},
                        {"name": "rule-2", "pattern": "**/fornavn", "func": "tink-daead(ssb-common-key-1)"},
                    ]
                },
                "targetContentType": "application/json",
            }
        )
        actual_request_json = find_multipart_obj("request", arg["files"])
        assert actual_request_json == expected_request_json
