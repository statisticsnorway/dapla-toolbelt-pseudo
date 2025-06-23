"""Utility functions for Dapla Pseudo."""

import asyncio
import typing as t
from datetime import date
from pathlib import Path

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
from dapla_pseudo.v1.mutable_dataframe import MutableDataFrame
from dapla_pseudo.v1.supported_file_format import SupportedOutputFileFormat


def find_multipart_obj(obj_name: str, multipart_files_tuple: set[t.Any]) -> t.Any:
    """Find "multipart object" by name.

    The requests lib specifies multipart file arguments as file-tuples, such as
    ('filename', fileobj, 'content_type')
    This method searches a tuple of such file-tuples ((file-tuple1),...,(file-tupleN))
    It returns the fileobj for the first matching file-tuple with a specified filename.

    Example:
    Given the multipart_files_tuple:
    multipart_tuple = (('filename1', fileobj1, 'application/json'), ('filename2', fileobj2, 'application/json'))

    then
    find_multipart_obj("filename2", multipart_tuple) -> fileobj
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
) -> tuple[str, list[str], RawPseudoMetadata]:
    """Perform the redact operation locally.

    This is in order to avoid making unnecessary requests to the API.
    """
    kwargs = t.cast(RedactKeywordArgs, request.pseudo_func.kwargs)
    if kwargs.placeholder is None:
        raise ValueError("Placeholder needs to be set for Redact")
    data = [kwargs.placeholder for _ in request.values]
    # The above operation could be vectorized using something like Polars,
    # however - the redact functionality is used mostly teams that use hierarchical
    # data, i.e. with very small lists. The overhead of
    # creating a Polars Series is probably not worth it.

    metadata = RawPseudoMetadata(
        field_name=request.name,
        logs=[],
        metrics=[],
        datadoc=[
            {
                "short_name": request.name.split("/")[-1],
                "data_element_path": request.name.replace("/", "."),
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


def asyncio_loop_running() -> bool:
    """Determins whether asyncio has a running event loop."""
    try:
        loop = asyncio.get_running_loop()
        if loop.is_running():
            return True
        else:
            return False
    except RuntimeError:
        return False


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
    match pseudo_operation:
        case PseudoOperation.PSEUDONYMIZE:
            for field in matched_fields.values():
                try:
                    req = PseudoFieldRequest(
                        pseudo_func=field.func,
                        name=field.path,
                        pattern=field.pattern,
                        values=field.get_value(),
                        keyset=KeyWrapper(custom_keyset).keyset,
                    )
                    requests.append(req)
                except ValidationError as e:
                    raise Exception(f"Path or column: {field.path}") from e
        case PseudoOperation.DEPSEUDONYMIZE:
            for field in matched_fields.values():
                try:
                    req = DepseudoFieldRequest(
                        pseudo_func=field.func,
                        name=field.path,
                        pattern=field.pattern,
                        values=field.get_value(),
                        keyset=KeyWrapper(custom_keyset).keyset,
                    )
                    requests.append(req)
                except ValidationError as e:
                    raise Exception(f"Path or column: {field.path}") from e

        case PseudoOperation.REPSEUDONYMIZE:
            if target_rules is not None:
                for field in matched_fields.values():
                    try:
                        req = RepseudoFieldRequest(
                            source_pseudo_func=field.func,
                            target_pseudo_func=field.target_func,
                            name=field.path,
                            pattern=field.pattern,
                            values=field.get_value(),
                            source_keyset=KeyWrapper(custom_keyset).keyset,
                            target_keyset=KeyWrapper(target_custom_keyset).keyset,
                        )
                        requests.append(req)
                    except ValidationError as e:
                        raise Exception(f"Path or column: {field.path}") from e
            else:
                raise ValueError("Found no target rules")
    return requests
