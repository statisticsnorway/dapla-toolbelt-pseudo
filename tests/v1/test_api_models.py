import json

from dapla_pseudo.constants import PseudoFunctionTypes
from dapla_pseudo.v1.api_models import DaeadKeywordArgs
from dapla_pseudo.v1.api_models import FF31KeywordArgs
from dapla_pseudo.v1.api_models import KeyWrapper
from dapla_pseudo.v1.api_models import PseudoFunction
from dapla_pseudo.v1.api_models import PseudoKeyset
from dapla_pseudo.v1.api_models import RedactArgs

TEST_FILE_PATH = "tests/v1/test_files"


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
