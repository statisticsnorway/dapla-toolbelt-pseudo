"""Utility functions for Dapla Pseudo."""

import asyncio
import json
import re
import typing as t
from datetime import date
from pathlib import Path

from datadoc_model.all_optional.model import Variable
from pydantic import ValidationError

from dapla_pseudo.constants import PseudoFunctionTypes
from dapla_pseudo.constants import PseudoOperation
from dapla_pseudo.exceptions import NoFileExtensionError
from dapla_pseudo.v1.models.api import DepseudoFieldRequest
from dapla_pseudo.v1.models.api import PseudoFieldRequest
from dapla_pseudo.v1.models.api import RawPseudoMetadata
from dapla_pseudo.v1.models.api import RepseudoFieldRequest
from dapla_pseudo.v1.models.core import KeyWrapper
from dapla_pseudo.v1.models.core import PseudoFunction
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
    requests: list[PseudoFieldRequest | DepseudoFieldRequest | RepseudoFieldRequest] = (
        []
    )
    req: PseudoFieldRequest | DepseudoFieldRequest | RepseudoFieldRequest

    grouped_matches = _group_matched_fields_for_requests(
        mutable_df=mutable_df,
        matched_fields=matched_fields,
    )

    match pseudo_operation:
        case PseudoOperation.PSEUDONYMIZE:
            for request_name, fields in grouped_matches:
                representative = fields[0]
                values = [value for field in fields for value in field.get_value()]
                try:
                    req = PseudoFieldRequest(
                        pseudo_func=representative.func,
                        name=request_name,
                        pattern=representative.pattern,
                        values=values,
                        keyset=KeyWrapper(custom_keyset).keyset,
                    )
                    requests.append(req)
                except ValidationError as e:
                    raise Exception(f"Path or column: {request_name}") from e
        case PseudoOperation.DEPSEUDONYMIZE:
            for request_name, fields in grouped_matches:
                representative = fields[0]
                values = [value for field in fields for value in field.get_value()]
                try:
                    req = DepseudoFieldRequest(
                        pseudo_func=representative.func,
                        name=request_name,
                        pattern=representative.pattern,
                        values=values,
                        keyset=KeyWrapper(custom_keyset).keyset,
                    )
                    requests.append(req)
                except ValidationError as e:
                    raise Exception(f"Path or column: {request_name}") from e

        case PseudoOperation.REPSEUDONYMIZE:
            if target_rules is not None:
                for request_name, fields in grouped_matches:
                    representative = fields[0]
                    values = [value for field in fields for value in field.get_value()]
                    try:
                        req = RepseudoFieldRequest(
                            source_pseudo_func=representative.func,
                            target_pseudo_func=representative.target_func,
                            name=request_name,
                            pattern=representative.pattern,
                            values=values,
                            source_keyset=KeyWrapper(custom_keyset).keyset,
                            target_keyset=KeyWrapper(target_custom_keyset).keyset,
                        )
                        requests.append(req)
                    except ValidationError as e:
                        raise Exception(f"Path or column: {request_name}") from e
            else:
                raise ValueError("Found no target rules")
    return requests


def _group_matched_fields_for_requests(
    mutable_df: MutableDataFrame,
    matched_fields: dict[str, FieldMatch],
) -> list[tuple[str, list[FieldMatch]]]:
    """Group matched fields into request-sized buckets.

    What we are trying to accomplish:
    - Avoid sending one tiny request per matched hierarchical leaf.

    Why:
    - Hierarchical traversal can produce many paths like
      ``identifiers[0]/foo``, ``identifiers[1]/foo``, etc.
    - Sending each as a separate HTTP call creates a lot of overhead.

    How:
    - In hierarchical mode, non-REDACT fields are batched when they share:
      normalized request path (without list indices), pattern, and pseudo function(s).
    - REDACT is intentionally *not* batched to preserve current metadata behavior.
    - For batched groups, we register a slice map in ``MutableDataFrame`` so one
      response can be split back into the original concrete leaf paths.
    """
    grouped: dict[tuple[str, str, str, str | None], list[FieldMatch]] = {}

    # 1) Bucket fields by "compatible request" identity.
    for field in matched_fields.values():
        should_batch = _should_batch_field(mutable_df, field)
        request_name = _remove_array_indices(field.path) if should_batch else field.path
        target_func = str(field.target_func) if field.target_func else None
        group_key = (request_name, field.pattern, str(field.func), target_func)
        grouped.setdefault(group_key, []).append(field)

    grouped_matches: list[tuple[str, list[FieldMatch]]] = []

    # 2) For groups with multiple members, register scatter metadata.
    for _, fields in grouped.items():
        representative = fields[0]
        request_name = _remove_array_indices(representative.path)
        should_register_batch = _should_batch_field(mutable_df, representative) and (
            len(fields) > 1
        )

        if should_register_batch:
            mutable_df.map_batch_to_leaf_slices(
                request_name,
                [(field.path, len(field.get_value())) for field in fields],
            )
            grouped_matches.append((request_name, fields))
        else:
            grouped_matches.append((representative.path, fields))

    return grouped_matches


def _should_batch_field(mutable_df: MutableDataFrame, field: FieldMatch) -> bool:
    """Return True when this field can safely participate in hierarchical batching."""
    if not mutable_df.hierarchical:
        return False

    # REDACT is kept at leaf-level granularity to preserve existing metadata output.
    if _is_redact_function(field.func) or _is_redact_function(field.target_func):
        return False

    return True


def _is_redact_function(func: PseudoFunction | None) -> bool:
    return func is not None and func.function_type == PseudoFunctionTypes.REDACT


def _remove_array_indices(path: str) -> str:
    return re.sub(r"\[\d+]", "", path)
