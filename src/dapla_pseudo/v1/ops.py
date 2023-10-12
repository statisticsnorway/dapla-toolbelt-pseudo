"""Pseudonymization operations provided to Dapla end users.

These operations aim to simplify the way Dapla users interact with the Dapla pseudo service. While the Dapla pseudo
service API offers many advanced options, like detailed configuration of pseudo rules on a field-basis targeting
hierarchical data structures, many users will be just fine with using one key and just listing the fields (of their
flat data).
"""
import io
import mimetypes
import os
import typing as t
from datetime import date
from pathlib import Path

import fsspec.spec


# isort: off
import pylibmagic  # noqa Must be imported before magic

# isort: on
import magic
import pandas as pd
import requests

from dapla_pseudo.constants import Env
from dapla_pseudo.constants import PredefinedKeys
from dapla_pseudo.constants import PseudoFunctionTypes
from dapla_pseudo.types import BinaryFileDecl
from dapla_pseudo.types import DatasetDecl
from dapla_pseudo.types import FieldDecl
from dapla_pseudo.utils import convert_to_date
from dapla_pseudo.v1.client import PseudoClient
from dapla_pseudo.v1.models import DepseudonymizeFileRequest
from dapla_pseudo.v1.models import FF31KeywordArgs
from dapla_pseudo.v1.models import Field
from dapla_pseudo.v1.models import KeyWrapper
from dapla_pseudo.v1.models import MapSidKeywordArgs
from dapla_pseudo.v1.models import Mimetypes
from dapla_pseudo.v1.models import PseudoConfig
from dapla_pseudo.v1.models import PseudoFunction
from dapla_pseudo.v1.models import PseudoKeyset
from dapla_pseudo.v1.models import PseudonymizeFileRequest
from dapla_pseudo.v1.models import PseudoRule
from dapla_pseudo.v1.models import RepseudonymizeFileRequest


def pseudonymize(
    dataset: DatasetDecl,
    fields: t.Optional[t.List[FieldDecl]] = None,
    sid_fields: t.Optional[t.List[str]] = None,
    sid_snapshot_date: t.Optional[str | date] = None,
    key: t.Union[str, PseudoKeyset] = PredefinedKeys.SSB_COMMON_KEY_1,
    timeout: t.Optional[int] = None,
    stream: bool = True,
) -> requests.Response:
    r"""Pseudonymize specified fields of a dataset.

    The dataset may be supplied as:
        - A local file on disk (string or Path)
        - A file handle (io.BufferedReader)
        - A Pandas dataframe

    Supported file formats: json, csv

    The ``fields`` and ``sid_fields`` lists specify what to pseudonymize. At least one of these fields must be specified.
    The list contents can either be plain field names (e.g. ``["some_field1", "another_field2"]``, or you can apply
    more advanced techniques, such as using wildcard characters (e.g. *Name) or slashes to target hierarchical fields
    (e.g. **/path/to/hierarchicalStuff).

    For ``fields``, the ``daead`` pseudonymization function is used. It requires a key.
    You can choose to specify one of the predefined ("globally available") keys ("ssb-common-key-1" or
    "ssb-common-key-2") or provide your own custom keyset. If you don't specify a key, the predefined "ssb-common-key-1"
    will be used as default.

    For ``sid_fields``, the ``map-sid`` pseudonymization function is used. This maps a fødselsnummer to a "stabil ID" and
    subsequently pseudonymizes the stabil ID using an FPE algorithm. Pseudonyms produced by this function are guaranteed to be
    compatible with those produced by the PAPIS project.

    It is possible to operate on the file in a streaming manner, e.g. like so:

    .. code-block:: python

        with pseudonymize("./data/personer.json", fields=["fnr", "fornavn", "etternavn"], stream=True) as res:
            with open("./data/personer.json", 'wb') as f:
                shutil.copyfileobj(res.raw, f)

    :param dataset: path to file, file handle or dataframe
    :param fields: list of fields that should be pseudonymized
    :param sid_fields: list of fields that should be mapped to stabil ID and pseudonymized
    :param sid_snapshot_date: Date representing SID-catalogue version to use. Latest if unspecified. Format: YYYY-MM-DD
    :param key: either named reference to a "global" key or a keyset json
    :param timeout: connection and read timeout, see
        https://requests.readthedocs.io/en/latest/user/advanced/?highlight=timeout#timeouts
    :param stream: true if the results should be chunked into pieces (use for large data)
    :return: pseudonymized data
    """
    if not fields and not sid_fields:
        raise ValueError("At least one of fields and sid_fields must be specified.")

    # Avoid later type errors by making sure we have lists
    if fields is None:
        fields = []
    if sid_fields is None:
        sid_fields = []

    file_handle: t.Optional[BinaryFileDecl] = None
    name: t.Optional[str] = None
    match dataset:
        case str() | Path():
            # File path
            content_type = Mimetypes(magic.from_file(dataset, mime=True))
        case pd.DataFrame():
            # Dataframe
            content_type = Mimetypes.JSON
            file_handle = _dataframe_to_json(dataset, fields, sid_fields)
        case io.BufferedReader():
            # File handle
            content_type = Mimetypes(magic.from_buffer(dataset.read(2048), mime=True))
            dataset.seek(0)
            file_handle = dataset
        case fsspec.spec.AbstractBufferedFile():
            # This is a file handle to a remote storage system such as GCS.
            # It provides random access for the underlying file-like data (without downloading the whole thing).
            content_type = Mimetypes(magic.from_buffer(dataset.read(2048), mime=True))
            name = dataset.path.split("/")[-1] if hasattr(dataset, "path") else None
            dataset.seek(0)
            file_handle = io.BufferedReader(dataset)
        case _:
            raise ValueError(f"Unsupported data type: {type(dataset)}. Supported types are {DatasetDecl}")
    k = KeyWrapper(key)
    sid_func_kwargs = MapSidKeywordArgs(snapshot_date=convert_to_date(sid_snapshot_date)) if sid_fields else None
    rules = _rules_of(fields=fields, sid_fields=sid_fields or [], key=k.key_id, sid_func_kwargs=sid_func_kwargs)
    pseudonymize_request = PseudonymizeFileRequest(
        pseudo_config=PseudoConfig(rules=rules, keysets=k.keyset_list()),
        target_content_type=content_type,
        target_uri=None,
        compression=None,
    )

    if file_handle is not None:
        return _client().pseudonymize_file(pseudonymize_request, file_handle, stream=stream, name=name, timeout=timeout)
    else:
        return _client()._process_file(
            "pseudonymize", pseudonymize_request, str(dataset), stream=stream, timeout=timeout
        )


def depseudonymize(
    file_path: str,
    fields: t.List[FieldDecl],
    key: t.Union[str, PseudoKeyset] = PredefinedKeys.SSB_COMMON_KEY_1,
    timeout: t.Optional[int] = None,
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
    :param timeout: connection and read timeout, see
        https://requests.readthedocs.io/en/latest/user/advanced/?highlight=timeout#timeouts
    :param stream: true if the results should be chunked into pieces (use for large data)
    :return: depseudonymized data
    """
    content_type = mimetypes.MimeTypes().guess_type(file_path)[0]
    k = KeyWrapper(key)
    rules = _rules_of(fields=fields, sid_fields=[], key=k.key_id)
    req = DepseudonymizeFileRequest(
        pseudo_config=PseudoConfig(rules=rules, keysets=k.keyset_list()),
        target_content_type=content_type,
        target_uri=None,
        compression=None,
    )

    return _client().depseudonymize_file(req, file_path, stream=stream, timeout=timeout)


def repseudonymize(
    file_path: str,
    fields: t.List[FieldDecl],
    source_key: t.Union[str, PseudoKeyset] = PredefinedKeys.SSB_COMMON_KEY_1,
    target_key: t.Union[str, PseudoKeyset] = PredefinedKeys.SSB_COMMON_KEY_1,
    timeout: t.Optional[int] = None,
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
    :param timeout: connection and read timeout, see
        https://requests.readthedocs.io/en/latest/user/advanced/?highlight=timeout#timeouts
    :param stream: true if the results should be chunked into pieces (use for large data)
    :return: repseudonymized data
    """
    content_type = _content_type_of(file_path)
    source_key_wrapper = KeyWrapper(source_key)
    target_key_wrapper = KeyWrapper(target_key)
    source_rules = _rules_of(fields=fields, sid_fields=[], key=source_key_wrapper.key_id)
    target_rules = _rules_of(fields=fields, sid_fields=[], key=target_key_wrapper.key_id)
    req = RepseudonymizeFileRequest(
        source_pseudo_config=PseudoConfig(rules=source_rules, keysets=source_key_wrapper.keyset_list()),
        target_pseudo_config=PseudoConfig(rules=target_rules, keysets=target_key_wrapper.keyset_list()),
        target_content_type=content_type,
        target_uri=None,
        compression=None,
    )

    return _client().repseudonymize_file(req, file_path, stream=stream, timeout=timeout)


def _client() -> PseudoClient:
    return PseudoClient(
        pseudo_service_url=os.getenv(Env.PSEUDO_SERVICE_URL),
        auth_token=os.getenv(Env.PSEUDO_SERVICE_AUTH_TOKEN),
    )


def _rules_of(
    fields: t.List[FieldDecl],
    sid_fields: t.List[str],
    key: str,
    sid_func_kwargs: t.Optional[MapSidKeywordArgs] = None,
) -> t.List[PseudoRule]:
    enriched_sid_fields: t.List[Field] = [Field(pattern=f"**/{field}", mapping="sid") for field in sid_fields]
    return [_rule_of(field, i, key, sid_func_kwargs) for i, field in enumerate(enriched_sid_fields + fields, 1)]


def _rule_of(f: FieldDecl, n: int, k: str, sid_func_kwargs: t.Optional[MapSidKeywordArgs] = None) -> PseudoRule:
    key = PredefinedKeys.SSB_COMMON_KEY_1 if k is None else k

    match f:
        case Field():
            field = f
        case dict():
            field = Field.model_validate(f)
        case str():
            field = Field(pattern=f"**/{f}")

    if field.mapping == "sid":
        func = PseudoFunction(
            function_type=PseudoFunctionTypes.MAP_SID,
            kwargs=sid_func_kwargs if sid_func_kwargs else MapSidKeywordArgs(),
        )
    elif key == "papis-common-key-1":
        func = PseudoFunction(function_type=PseudoFunctionTypes.FF31, kwargs=FF31KeywordArgs(key_id=key))
    else:
        func = PseudoFunction(function_type=PseudoFunctionTypes.DAEAD, kwargs=MapSidKeywordArgs(key_id=key))

    return PseudoRule(
        name=f"rule-{n}",
        func=func,
        pattern=field.pattern,
    )


def _content_type_of(file_path: str) -> str:
    return str(mimetypes.MimeTypes().guess_type(file_path)[0])


def _dataframe_to_json(
    data: pd.DataFrame,
    fields: t.Optional[t.Sequence[FieldDecl]] = None,
    sid_fields: t.Optional[t.Sequence[str]] = None,
) -> t.BinaryIO:
    # Ensure fields to be pseudonymized are string type
    for field in tuple(fields or []) + tuple(sid_fields or []):
        if isinstance(field, str):
            data[field] = data[field].apply(str)

    file_handle = io.BytesIO()
    data.to_json(file_handle, orient="records")
    file_handle.seek(0)
    return file_handle
