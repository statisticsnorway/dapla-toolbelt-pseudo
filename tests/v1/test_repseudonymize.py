import json
from unittest import mock

import pytest

from dapla_pseudo import repseudonymize
from dapla_pseudo.constants import Env
from dapla_pseudo.constants import PredefinedKeys
from dapla_pseudo.v1.models import PseudoKeyset


base_url = "https://mocked.dapla-pseudo-service"
auth_token = "some-auth-token"

custom_keyset = PseudoKeyset.model_validate(
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


def test_repseudonymize_request_with_default_keys(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(Env.PSEUDO_SERVICE_URL, base_url)
    monkeypatch.setenv(Env.PSEUDO_SERVICE_AUTH_TOKEN, auth_token)

    with mock.patch("requests.post") as patched:
        repseudonymize(file_path="tests/data/personer.json", fields=["fnr", "fornavn"])
        patched.assert_called_once()
        arg = patched.call_args.kwargs

        assert arg["url"] == f"{base_url}/repseudonymize/file"
        assert arg["headers"]["Authorization"] == f"Bearer {auth_token}"
        assert arg["stream"] is True

        expected_request_dict = {
            "sourcePseudoConfig": {
                "rules": [
                    {"name": "rule-1", "pattern": "**/fnr", "func": "daead(keyId=ssb-common-key-1)"},
                    {"name": "rule-2", "pattern": "**/fornavn", "func": "daead(keyId=ssb-common-key-1)"},
                ]
            },
            "targetPseudoConfig": {
                "rules": [
                    {"name": "rule-1", "pattern": "**/fnr", "func": "daead(keyId=ssb-common-key-1)"},
                    {"name": "rule-2", "pattern": "**/fornavn", "func": "daead(keyId=ssb-common-key-1)"},
                ]
            },
            "targetContentType": "application/json",
        }

        actual_request_dict = json.loads(arg["files"]["request"][1])
        assert actual_request_dict == expected_request_dict


def test_repseudonymize_request_with_explicitly_specified_common_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(Env.PSEUDO_SERVICE_URL, base_url)
    monkeypatch.setenv(Env.PSEUDO_SERVICE_AUTH_TOKEN, auth_token)

    with mock.patch("requests.post") as patched:
        repseudonymize(
            file_path="tests/data/personer.json",
            fields=["fnr", "fornavn"],
            source_key=PredefinedKeys.SSB_COMMON_KEY_1,
            target_key=PredefinedKeys.SSB_COMMON_KEY_2,
        )
        patched.assert_called_once()
        arg = patched.call_args.kwargs

        assert arg["url"] == f"{base_url}/repseudonymize/file"
        assert arg["headers"]["Authorization"] == f"Bearer {auth_token}"
        assert arg["stream"] is True

        expected_request_dict = {
            "sourcePseudoConfig": {
                "rules": [
                    {"name": "rule-1", "pattern": "**/fnr", "func": "daead(keyId=ssb-common-key-1)"},
                    {"name": "rule-2", "pattern": "**/fornavn", "func": "daead(keyId=ssb-common-key-1)"},
                ]
            },
            "targetPseudoConfig": {
                "rules": [
                    {"name": "rule-1", "pattern": "**/fnr", "func": "daead(keyId=ssb-common-key-2)"},
                    {"name": "rule-2", "pattern": "**/fornavn", "func": "daead(keyId=ssb-common-key-2)"},
                ]
            },
            "targetContentType": "application/json",
        }

        actual_request_dict = json.loads(arg["files"]["request"][1])
        assert actual_request_dict == expected_request_dict


def test_repseudonymize_request_with_explicitly_specified_keyset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(Env.PSEUDO_SERVICE_URL, base_url)
    monkeypatch.setenv(Env.PSEUDO_SERVICE_AUTH_TOKEN, auth_token)

    with mock.patch("requests.post") as patched:
        repseudonymize(
            file_path="tests/data/personer.json",
            fields=["fnr", "fornavn"],
            target_key=custom_keyset,
        )
        patched.assert_called_once()
        arg = patched.call_args.kwargs

        assert arg["url"] == f"{base_url}/repseudonymize/file"
        assert arg["headers"]["Authorization"] == f"Bearer {auth_token}"
        assert arg["stream"] is True

        expected_request_dict = {
            "sourcePseudoConfig": {
                "rules": [
                    {"name": "rule-1", "pattern": "**/fnr", "func": "daead(keyId=ssb-common-key-1)"},
                    {"name": "rule-2", "pattern": "**/fornavn", "func": "daead(keyId=ssb-common-key-1)"},
                ]
            },
            "targetPseudoConfig": {
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

        actual_request_dict = json.loads(arg["files"]["request"][1])
        assert actual_request_dict == expected_request_dict
