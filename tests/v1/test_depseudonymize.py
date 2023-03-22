import json
from unittest import mock

import pytest

from dapla_pseudo import depseudonymize
from dapla_pseudo.constants import env


base_url = "https://mocked.dapla-pseudo-service"
auth_token = "some-auth-token"


def test_depseudonymize_request_with_default_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(env.PSEUDO_SERVICE_URL, base_url)
    monkeypatch.setenv(env.PSEUDO_SERVICE_AUTH_TOKEN, auth_token)

    with mock.patch("requests.post") as patched:
        depseudonymize(file_path="tests/data/personer.json", fields=["fnr", "fornavn"])
        patched.assert_called_once()
        arg = patched.call_args.kwargs

        assert arg["url"] == f"{base_url}/depseudonymize/file"
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
