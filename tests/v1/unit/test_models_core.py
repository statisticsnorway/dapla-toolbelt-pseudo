import json

from dapla_pseudo.constants import PseudoFunctionTypes
from dapla_pseudo.constants import UnknownCharacterStrategy
from dapla_pseudo.v1.models.core import DaeadKeywordArgs
from dapla_pseudo.v1.models.core import FF31KeywordArgs
from dapla_pseudo.v1.models.core import KeyWrapper
from dapla_pseudo.v1.models.core import MapSidKeywordArgs
from dapla_pseudo.v1.models.core import PseudoFunction
from dapla_pseudo.v1.models.core import PseudoKeyset
from dapla_pseudo.v1.models.core import PseudoRule
from dapla_pseudo.v1.models.core import RedactKeywordArgs

TEST_FILE_PATH = "tests/v1/unit/test_files"


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


def test_serialize_daead_function() -> None:
    assert str(
        PseudoFunction(
            function_type=PseudoFunctionTypes.DAEAD, kwargs=DaeadKeywordArgs()
        )
        == "daead(keyId=ssb-common-key-1)"
    )


def test_deserialize_daead_function() -> None:
    assert PseudoFunction.model_validate("daead(keyId=ssb-common-key-1)") == (
        PseudoFunction(
            function_type=PseudoFunctionTypes.DAEAD, kwargs=DaeadKeywordArgs()
        )
    )


def test_deserialize_empty_daead_function() -> None:
    assert PseudoFunction.model_validate("daead()") == (
        PseudoFunction(
            function_type=PseudoFunctionTypes.DAEAD, kwargs=DaeadKeywordArgs()
        )
    )


def test_serialize_redact_function() -> None:
    assert str(
        PseudoFunction(
            function_type=PseudoFunctionTypes.REDACT,
            kwargs=RedactKeywordArgs(placeholder="#"),
        )
        == "redact(placeholder=#)"
    )


def test_deserialize_redact_function() -> None:
    assert PseudoFunction.model_validate("redact(placeholder=#)") == (
        PseudoFunction(
            function_type=PseudoFunctionTypes.REDACT,
            kwargs=RedactKeywordArgs(placeholder="#"),
        )
    )


def test_serialize_function_with_extra_kwargs() -> None:
    assert str(
        PseudoFunction(
            function_type=PseudoFunctionTypes.FF31,
            kwargs=FF31KeywordArgs(),
        )
        == "ff31(keyId=papis-common-key-1,strategy=skip)"
    )


def test_deserialize_function_with_extra_kwargs() -> None:
    assert PseudoFunction.model_validate(
        "ff31(keyId=papis-common-key-1,strategy=skip)"
    ) == (
        PseudoFunction(
            function_type=PseudoFunctionTypes.FF31,
            kwargs=FF31KeywordArgs(),
        )
    )


def test_deserialize_pseudo_rule() -> None:
    assert PseudoRule.from_json(
        '{"name":"my-rule","pattern":"**/identifiers/*","func":"ff31('
        'keyId=papis-common-key-1,strategy=redact)"}'
    ) == (
        PseudoRule(
            name="my-rule",
            func=PseudoFunction(
                function_type=PseudoFunctionTypes.FF31,
                kwargs=FF31KeywordArgs(strategy=UnknownCharacterStrategy.REDACT),
            ),
            pattern="**/identifiers/*",
        )
    )


def test_deserialize_pseudo_rule_with_defaults() -> None:
    assert PseudoRule.from_json(
        '{"name":"my-rule","pattern":"**/identifiers/*","func":"ff31()"}'
    ) == (
        PseudoRule(
            name="my-rule",
            func=PseudoFunction(
                function_type=PseudoFunctionTypes.FF31,
                kwargs=FF31KeywordArgs(),
            ),
            pattern="**/identifiers/*",
        )
    )


def test_deserialize_map_sid_pseudo_rule_with_defaults() -> None:
    assert PseudoRule.from_json(
        '{"name":"my-rule","pattern":"**/identifiers/*","func":"map-sid-ff31()"}'
    ) == (
        PseudoRule(
            name="my-rule",
            func=PseudoFunction(
                function_type=PseudoFunctionTypes.MAP_SID,
                kwargs=MapSidKeywordArgs(),
            ),
            pattern="**/identifiers/*",
        )
    )
