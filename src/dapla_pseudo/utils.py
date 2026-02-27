"""Utility functions for Dapla Pseudo."""

import asyncio
import json
import re
import typing as t
from datetime import date
from pathlib import Path

from datadoc_model.all_optional.model import Variable
from pydantic import ValidationError

from dapla_pseudo.constants import PseudoOperation
from dapla_pseudo.exceptions import NoFileExtensionError
from dapla_pseudo.v1.models.api import DepseudoFieldRequest
from dapla_pseudo.v1.models.api import PseudoFieldRequest
from dapla_pseudo.v1.models.api import RawPseudoMetadata
from dapla_pseudo.v1.models.api import RepseudoFieldRequest
from dapla_pseudo.v1.models.core import KeyWrapper
from dapla_pseudo.v1.models.core import PseudoKeyset
from dapla_pseudo.v1.models.core import PseudoRule
from dapla_pseudo.v1.models.core import RedactKeywordArgs
from dapla_pseudo.v1.mutable_dataframe import FieldMatch
from dapla_pseudo.v1.mutable_dataframe import MutableDataFrame
from dapla_pseudo.v1.supported_file_format import SupportedOutputFileFormat


def encode_datadoc_variables(variables: list[Variable], indent: int = 2) -> str:
    """Encore datadoc variables to a fromatted json list."""
    return json.dumps(
        [v.model_dump(exclude_none=True) for v in variables], indent=indent
    )


def find_multipart_obj(obj_name: str, multipart_files_tuple: set[t.Any]) -> t.Any:
    """Find "multipart object" by name.

    The requests lib specifies multipart file arguments as file-tuples, such as
    ('filename', fileobj, 'content_type')
    This method searches a tuple of such file-tuples ((file-tuple1),...,(file-tupleN))
    It returns the fileobj for the first matching file-tuple with a specified filename.

    Args:
        obj_name: The name of the object
        multipart_files_tuple: The multipart tuple

    Returns:
        The fileobject associated with the matched tuple

    Example:
        ```
        multipart_tuple = (('filename1', fileobj1, 'application/json'), ('filename2', fileobj2, 'application/json'))

        find_multipart_obj("filename2", multipart_tuple) -> fileobj2
        ```
    """
    try:
        matching_item = next(
            item[1] for item in multipart_files_tuple if item[0] == obj_name
        )
        return matching_item[1]
    except StopIteration:
        return None


def redact_field(
    request: PseudoFieldRequest,
) -> tuple[str, list[str | None], RawPseudoMetadata]:
    """Perform the redact operation locally.

    This is in order to avoid making unnecessary requests to the API.
    """

    def _remove_brackets_after_last_slash(text: str) -> str:
        if "/" not in text:
            return re.sub(r"\[.*?\]", "", text)  # fallback if no slashes at all

        # Split at the last slash
        before, after = text.rsplit("/", 1)
        # Remove bracketed substrings only in the "after" part
        after_cleaned = re.sub(r"\[.*?\]", "", after)
        # Recombine and return
        return f"{before}/{after_cleaned}"

    kwargs = t.cast(RedactKeywordArgs, request.pseudo_func.kwargs)
    if kwargs.placeholder is None:
        raise ValueError("Placeholder needs to be set for Redact")
    data: list[str | None] = [kwargs.placeholder for _ in request.values]
    # The above operation could be vectorized using something like Polars,
    # however - the redact functionality is used mostly teams that use hierarchical
    # data, i.e. with very small lists. The overhead of
    # creating a Polars Series is probably not worth it.
    name_no_indices = _remove_brackets_after_last_slash(request.name)
    metadata = RawPseudoMetadata(
        field_name=name_no_indices,
        logs=[],
        metrics=[],
        datadoc=[
            {
                "short_name": name_no_indices.split("/")[-1],
                "data_element_path": name_no_indices.replace("/", "."),
                "pseudonymization": {
                    "encryption_algorithm": "REDACT",
                    "encryption_algorithm_parameters": [
                        request.pseudo_func.kwargs.model_dump(exclude_none=True)
                    ],
                },
            }
        ],
    )

    return request.name, data, metadata


def running_asyncio_loop() -> asyncio.AbstractEventLoop | None:
    """Returns the asyncio event loop if it exists."""
    try:
        loop = asyncio.get_running_loop()
        if loop.is_running():
            return loop
        else:
            return None
    except RuntimeError:
        return None


def convert_to_date(sid_snapshot_date: date | str | None = None) -> date | None:
    """Converts the SID version date to the 'date' type, if it is a string.

    If None, simply passes the None through the function.
    """
    if isinstance(sid_snapshot_date, str):
        try:
            return date.fromisoformat(sid_snapshot_date)
        except ValueError as exc:
            raise ValueError(
                "Version timestamp must be a valid ISO date string (YYYY-MM-DD)"
            ) from exc
    return sid_snapshot_date


def get_file_format_from_file_name(file_path: str | Path) -> SupportedOutputFileFormat:
    """Extracts the file format from a file path."""
    if isinstance(file_path, str):
        file_path = Path(file_path)

    file_extension = file_path.suffix
    if not file_extension:
        raise NoFileExtensionError(f"File path '{file_path}' has no file extension.")
    file_format = SupportedOutputFileFormat(file_extension.replace(".", ""))
    return file_format


def build_pseudo_field_request(
    pseudo_operation: PseudoOperation,
    mutable_df: MutableDataFrame,
    rules: list[PseudoRule],  # "source rules" if repseudo
    custom_keyset: PseudoKeyset | str | None = None,
    target_custom_keyset: PseudoKeyset | str | None = None,  # used in repseudo
    target_rules: list[PseudoRule] | None = None,  # used in repseudo)
) -> list[PseudoFieldRequest | DepseudoFieldRequest | RepseudoFieldRequest]:
    """Builds a FieldRequest object."""
    mutable_df.match_rules(rules, target_rules)
    matched_fields = mutable_df.get_matched_fields()
    if mutable_df.hierarchical:
        return _build_hierarchical_field_requests(
            pseudo_operation=pseudo_operation,
            mutable_df=mutable_df,
            matched_fields=matched_fields,
            custom_keyset=custom_keyset,
            target_custom_keyset=target_custom_keyset,
            target_rules=target_rules,
        )

    else:
        return _build_tabular_field_requests(
            pseudo_operation=pseudo_operation,
            matched_fields=matched_fields,
            custom_keyset=custom_keyset,
            target_custom_keyset=target_custom_keyset,
            target_rules=target_rules,
        )


def _build_tabular_field_requests(
    pseudo_operation: PseudoOperation,
    matched_fields: dict[str, FieldMatch],
    custom_keyset: PseudoKeyset | str | None,
    target_custom_keyset: PseudoKeyset | str | None,
    target_rules: list[PseudoRule] | None,
) -> list[PseudoFieldRequest | DepseudoFieldRequest | RepseudoFieldRequest]:
    return [
        _build_single_field_request(
            pseudo_operation=pseudo_operation,
            request_name=field.path,
            representative=field,
            values=field.get_value(),
            custom_keyset=custom_keyset,
            target_custom_keyset=target_custom_keyset,
            target_rules=target_rules,
        )
        for field in matched_fields.values()
    ]


def _build_hierarchical_field_requests(
    pseudo_operation: PseudoOperation,
    mutable_df: MutableDataFrame,
    matched_fields: dict[str, FieldMatch],
    custom_keyset: PseudoKeyset | str | None,
    target_custom_keyset: PseudoKeyset | str | None,
    target_rules: list[PseudoRule] | None,
) -> list[PseudoFieldRequest | DepseudoFieldRequest | RepseudoFieldRequest]:
    grouped_matches = _group_hierarchical_fields_for_requests(
        mutable_df=mutable_df,
        matched_fields=matched_fields,
    )
    return [
        _build_single_field_request(
            pseudo_operation=pseudo_operation,
            request_name=request_name,
            representative=fields[0],
            values=[value for field in fields for value in field.get_value()],
            custom_keyset=custom_keyset,
            target_custom_keyset=target_custom_keyset,
            target_rules=target_rules,
        )
        for request_name, fields in grouped_matches
    ]


def _build_single_field_request(
    pseudo_operation: PseudoOperation,
    request_name: str,
    representative: FieldMatch,
    values: list[str | int | None],
    custom_keyset: PseudoKeyset | str | None,
    target_custom_keyset: PseudoKeyset | str | None,
    target_rules: list[PseudoRule] | None,
) -> PseudoFieldRequest | DepseudoFieldRequest | RepseudoFieldRequest:
    req: PseudoFieldRequest | DepseudoFieldRequest | RepseudoFieldRequest

    try:
        match pseudo_operation:
            case PseudoOperation.PSEUDONYMIZE:
                req = PseudoFieldRequest(
                    pseudo_func=representative.func,
                    name=request_name,
                    pattern=representative.pattern,
                    values=values,
                    keyset=KeyWrapper(custom_keyset).keyset,
                )
            case PseudoOperation.DEPSEUDONYMIZE:
                req = DepseudoFieldRequest(
                    pseudo_func=representative.func,
                    name=request_name,
                    pattern=representative.pattern,
                    values=values,
                    keyset=KeyWrapper(custom_keyset).keyset,
                )
            case PseudoOperation.REPSEUDONYMIZE:
                if target_rules is None:
                    raise ValueError("Found no target rules")

                req = RepseudoFieldRequest(
                    source_pseudo_func=representative.func,
                    target_pseudo_func=representative.target_func,
                    name=request_name,
                    pattern=representative.pattern,
                    values=values,
                    source_keyset=KeyWrapper(custom_keyset).keyset,
                    target_keyset=KeyWrapper(target_custom_keyset).keyset,
                )
    except ValidationError as e:
        raise Exception(f"Path or column: {request_name}") from e

    return req


def _group_hierarchical_fields_for_requests(
    mutable_df: MutableDataFrame,
    matched_fields: dict[str, FieldMatch],
) -> list[tuple[str, list[FieldMatch]]]:
    """Group hierarchical field matches into pseudo-service requests.

    Example input paths:
    - ``person_info[0]/fnr``
    - ``person_info[1]/fnr``
    - ``person_info[2]/fnr``

    These are grouped into one request named ``person_info/fnr``. The grouped
    request contains all values from all matching leaf paths.

    Two paths share a request only when all of these match:
    - normalized request name (array indices removed)
    - pattern
    - source pseudo function
    - target pseudo function (for repseudonymize)

    """
    grouped: dict[tuple[str, str, str, str | None], list[FieldMatch]] = {}

    # Group paths that can share one API request.
    for field in matched_fields.values():
        request_name = _remove_array_indices(field.path)
        target_func = str(field.target_func) if field.target_func else None
        group_key = (request_name, field.pattern, str(field.func), target_func)
        grouped.setdefault(group_key, []).append(field)

    grouped_matches: list[tuple[str, list[FieldMatch]]] = []

    # If a group is batched, store slice boundaries so one response list can be
    # written back to the original leaf paths.
    for _, fields in grouped.items():
        representative = fields[0]
        request_name = _remove_array_indices(representative.path)

        if len(fields) > 1:
            mutable_df.map_batch_to_leaf_slices(
                request_name,
                [(field.path, len(field.get_value())) for field in fields],
            )
            grouped_matches.append((request_name, fields))
        else:
            grouped_matches.append((representative.path, fields))

    return grouped_matches


def _remove_array_indices(path: str) -> str:
    return re.sub(r"\[\d+]", "", path)
