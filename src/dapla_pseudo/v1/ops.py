"""Pseudonymization operations provided to Dapla end users.

These operations aim to simplify the way Dapla users interact with the Dapla pseudo service. While the Dapla pseudo
service API offers many advanced options, like detailed configuration of pseudo rules on a field-basis targeting
hierarchical data structures, many users will be just fine with using one key and just listing the fields (of their
flat data).
"""
import mimetypes
import os
import typing as t

import requests

from dapla_pseudo.constants import env
from dapla_pseudo.constants import predefined_keys

from .client import PseudoClient
from .models import DepseudonymizeFileRequest
from .models import Field
from .models import KeyWrapper
from .models import PseudoConfig
from .models import PseudoKeyset
from .models import PseudonymizeFileRequest
from .models import PseudoRule
from .models import RepseudonymizeFileRequest


_FieldDecl = t.Union[str, dict, Field]


def pseudonymize(
    file_path: str,
    fields: t.List[_FieldDecl],
    sid: t.Optional[t.List[str]] = None,
    key: t.Union[str, PseudoKeyset] = predefined_keys.SSB_COMMON_KEY_1,
    stream: bool = True,
) -> requests.Response:
    """Pseudonymize specified fields of a local file.

    Supported file formats: csv and json (both standard and "new line delimited" json)

    You can alternatively send a zip-file containing one or many files of the supported file formats. The pseudo service
    will unzip and process them sequentially. This can be handy if your file is large and/or split into multiple files.

    The ``fields`` list specifies what to pseudonymize. This can either be a plain vanilla list of field names (e.g.
    ``["some_field1", "another_field2"]``, or you can apply ninja-style techniques, such as using wildcard characters
    (e.g. *Name) or slashes to target hierarchical fields (e.g. **/path/to/hierarchicalStuff).

    Pseudonymize uses the tink-daead crypto function underneath the hood. It requires a key.
    You can choose to specify one of the predefined ("globally available") keys ("ssb-common-key-1" or
    "ssb-common-key-2") or provide your own custom keyset. If you don't specify a key, the predefined "ssb-common-key-1"
    will be used as default.

    It is possible to operate on the file in a streaming manner, e.g. like so:

    .. code-block:: python

        with pseudonymize("./data/personer.json", fields=["fnr", "fornavn", "etternavn"], stream=True) as res:
            with open("./data/personer.json", 'wb') as f:
                shutil.copyfileobj(res.raw, f)

    :param file_path: path to a local file, e.g. ./path/to/data.json. Supported file formats: csv, json
    :param fields: list of fields that should be pseudonymized
    :param sid: list of fields that should be mapped to stabil ID and pseudonymized
    :param key: either named reference to a "global" key or a keyset json
    :param stream: true if the results should be chunked into pieces (use for large data)
    :return: pseudonymized data
    """
    content_type = _content_type_of(file_path)
    k = KeyWrapper(key)
    rules = _rules_of(fields=fields, sid=sid or [], key=k.key_id)
    req = PseudonymizeFileRequest(
        pseudo_config=PseudoConfig(rules=rules, keysets=k.keyset_list()), target_content_type=content_type
    )

    return _client().pseudonymize_file(req.to_json(), file_path, stream=stream)


def depseudonymize(
    file_path: str,
    fields: t.List[_FieldDecl],
    key: t.Union[str, PseudoKeyset] = predefined_keys.SSB_COMMON_KEY_1,
    stream: bool = True,
) -> requests.Response:
    """Depseudonymize specified fields of a local file.

    This is the inverse operation of "pseudonymize". Special privileges will be required (e.g. only whitelisted users)
    will be allowed to depseudonymize data.

    Supported file formats: csv and json (both standard and "new line delimited" json)

    You can alternatively send a zip-file containing one or many files of the supported file formats. The pseudo service
    will unzip and process them sequentially. This can be handy if your file is large and/or split into multiple files.

    The ``fields`` list specifies what to pseudonymize. This can either be a plain vanilla list of field names (e.g.
    ``["some_field1", "another_field2"]``, or you can apply ninja-style techniques, such as using wildcard characters
    (e.g. *Name) or slashes to target hierarchical fields (e.g. **/path/to/hierarchicalStuff).

    Pseudonymize uses the tink-daead crypto function underneath the hood. It requires a key.
    You can choose to specify one of the predefined ("globally available") keys ("ssb-common-key-1" or
    "ssb-common-key-2") or provide your own custom keyset. If you don't specify a key, the predefined "ssb-common-key-1"
    will be used as default.

    It is possible to operate on the file in a streaming manner, e.g. like so:

    .. code-block:: python

        with depseudonymize("./data/personer.json", fields=["fnr", "fornavn", "etternavn"], stream=True) as res:
            with open("./data/personer_deid.json", 'wb') as f:
                shutil.copyfileobj(res.raw, f)

    :param file_path: path to a local file, e.g. ./path/to/data-deid.json. Supported file formats: csv, json
    :param fields: list of fields that should be depseudonymized
    :param key: either named reference to a "global" key or a keyset json
    :param stream: true if the results should be chunked into pieces (use for large data)
    :return: depseudonymized data
    """
    content_type = mimetypes.MimeTypes().guess_type(file_path)[0]
    k = KeyWrapper(key)
    rules = _rules_of(fields=fields, sid=[], key=k.key_id)
    req = DepseudonymizeFileRequest(
        pseudo_config=PseudoConfig(rules=rules, keysets=k.keyset_list()),
        target_content_type=content_type,
    )

    return _client().depseudonymize_file(req.to_json(), file_path, stream=stream)


def repseudonymize(
    file_path: str,
    fields: t.List[_FieldDecl],
    source_key: t.Union[str, PseudoKeyset] = predefined_keys.SSB_COMMON_KEY_1,
    target_key: t.Union[str, PseudoKeyset] = predefined_keys.SSB_COMMON_KEY_1,
    stream: bool = True,
) -> requests.Response:
    """Repseudonymize specified fields of a local, previously pseudonymized file.

    You will need to provide a crypto key for both the source data and a key that should be used for
    re-pseudonymization.

    Supported file formats: csv and json (both standard and "new line delimited" json)

    You can alternatively send a zip-file containing one or many files of the supported file formats. The pseudo service
    will unzip and process them sequentially. This can be handy if your file is large and/or split into multiple files.

    The ``fields`` list specifies what to repseudonymize. This can either be a plain vanilla list of field names (e.g.
    ``["some_field1", "another_field2"]``, or you can apply ninja-style techniques, such as using wildcard characters
    (e.g. *Name) or slashes to target hierarchical fields (e.g. **/path/to/hierarchicalStuff).

    Pseudonymize uses the tink-daead crypto function underneath the hood. It requires a key.
    You can choose to specify one of the predefined ("globally available") keys ("ssb-common-key-1" or
    "ssb-common-key-2") or provide your own custom keyset. If you don't specify a key, the predefined "ssb-common-key-1"
    will be used as default.

    It is possible to operate on the file in a streaming manner, e.g. like so:

    .. code-block:: python

        with repseudonymize("./data/personer-deid.json", fields=["fnr", "fornavn", "etternavn"], stream=True) as res:
            with open("./data/personer.json", 'wb') as f:
                shutil.copyfileobj(res.raw, f)

    :param file_path: path to a local file, e.g. ./path/to/data.json. Supported file formats: csv, json
    :param fields: list of fields that should be pseudonymized
    :param source_key: either named reference to a "global" key or a keyset json - used for depseudonymization
    :param target_key: either named reference to a "global" key or a keyset json - used for pseudonymization
    :param stream: true if the results should be chunked into pieces (use for large data)
    :return: repseudonymized data
    """
    content_type = _content_type_of(file_path)
    source_key_wrapper = KeyWrapper(source_key)
    target_key_wrapper = KeyWrapper(target_key)
    source_rules = _rules_of(fields=fields, sid=[], key=source_key_wrapper.key_id)
    target_rules = _rules_of(fields=fields, sid=[], key=target_key_wrapper.key_id)
    req = RepseudonymizeFileRequest(
        source_pseudo_config=PseudoConfig(rules=source_rules, keysets=source_key_wrapper.keyset_list()),
        target_pseudo_config=PseudoConfig(rules=target_rules, keysets=target_key_wrapper.keyset_list()),
        target_content_type=content_type,
    )

    return _client().repseudonymize_file(req.to_json(), file_path, stream=stream)


def _client() -> PseudoClient:
    return PseudoClient(
        pseudo_service_url=os.getenv(env.PSEUDO_SERVICE_URL),
        auth_token=os.getenv(env.PSEUDO_SERVICE_AUTH_TOKEN),
    )


def _rules_of(fields: t.List[_FieldDecl], sid: t.List[str], key: str) -> t.List[PseudoRule]:
    sid_fields = [Field(pattern=f"**/{field}", mapping="sid") for field in sid]
    return [_rule_of(field, i, key) for i, field in enumerate(sid_fields + fields, 1)]


def _rule_of(f: _FieldDecl, n: int, k: str) -> PseudoRule:
    key = "ssb-common-key-1" if k is None else k

    if isinstance(f, Field):
        field = f
    elif isinstance(f, dict):
        field = Field.parse_obj(f)
    elif isinstance(f, str):
        field = Field(pattern=f"**/{f}")

    if field.mapping == "sid":
        func = f"map-sid({key})"
    else:
        func = f"tink-daead({key})"

    return PseudoRule(
        name=f"rule-{n}",
        func=func,
        pattern=field.pattern,
    )


def _content_type_of(file_path: str) -> str:
    return str(mimetypes.MimeTypes().guess_type(file_path)[0])
