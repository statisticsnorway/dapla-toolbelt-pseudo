import json
from unittest import mock

from dapla_pseudo import PseudoClient


base_url = "https://mocked.dapla-pseudo-service"
auth_token = "some-auth-token"


def test_export_dataset() -> None:
    client = PseudoClient(pseudo_service_url=base_url, auth_token=auth_token)
    request_json = json.dumps(
        {
            "sourceDataset": {"root": "gs://some-bucket", "path": "/path/to/some/data", "version": "123"},
            "targetContentName": "blah",
            "targetContentType": "application/json",
            "targetPassword": "kensentme",
            "depseudonymize": True,
            "pseudoRules": [
                {"name": "rule-1", "pattern": "**/{*Fnr,*Id}", "func": "tink-daead(ssb-common-key-1)"},
                {"name": "rule-2", "pattern": "**/path/to/ignorable/stuff/*", "func": "redact(***)"},
            ],
        }
    )

    with mock.patch("requests.post") as patched:
        client.export_dataset(request_json=request_json)
        patched.assert_called_once()
        arg = patched.call_args.kwargs

        assert arg["url"] == f"{base_url}/export"
        assert arg["headers"] == {"Authorization": f"Bearer {auth_token}", "Content-Type": "application/json"}
        assert arg["data"] == request_json
