"""Utility functions for Dapla Pseudo."""

import io
import os
import typing as t
import zipfile
from datetime import date
from pathlib import Path

import fsspec
import orjson
import polars as pl
from dapla import FileClient
from gcsfs.core import GCSFile
from google.auth.exceptions import DefaultCredentialsError
from pydantic import ValidationError

from dapla_pseudo.constants import PseudoOperation
from dapla_pseudo.exceptions import FileInvalidError
from dapla_pseudo.exceptions import MimetypeNotSupportedError
from dapla_pseudo.exceptions import NoFileExtensionError
from dapla_pseudo.types import BinaryFileDecl
from dapla_pseudo.types import FileLikeDatasetDecl
from dapla_pseudo.v1.models.api import DepseudoFieldRequest
from dapla_pseudo.v1.models.api import DepseudoFileRequest
from dapla_pseudo.v1.models.api import PseudoFieldRequest
from dapla_pseudo.v1.models.api import PseudoFileRequest
from dapla_pseudo.v1.models.api import RepseudoFieldRequest
from dapla_pseudo.v1.models.api import RepseudoFileRequest
from dapla_pseudo.v1.models.core import KeyWrapper
from dapla_pseudo.v1.models.core import Mimetypes
from dapla_pseudo.v1.models.core import PseudoConfig
from dapla_pseudo.v1.models.core import PseudoKeyset
from dapla_pseudo.v1.models.core import PseudoRule
from dapla_pseudo.v1.mutable_dataframe import MutableDataFrame
from dapla_pseudo.v1.supported_file_format import FORMAT_TO_MIMETYPE_FUNCTION
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


def build_pseudo_file_request(
    pseudo_operation: PseudoOperation,
    rules: list[PseudoRule],  # "source rules" if repseudo
    custom_keyset: PseudoKeyset | str | None = None,
    target_custom_keyset: PseudoKeyset | str | None = None,  # used in repseudo
    target_rules: list[PseudoRule] | None = None,  # used in repseudo)
) -> PseudoFileRequest | DepseudoFileRequest | RepseudoFileRequest:
    """Builds a file request object."""
    match pseudo_operation:
        case PseudoOperation.PSEUDONYMIZE:
            return PseudoFileRequest(
                pseudo_config=PseudoConfig(
                    rules=rules,
                    keysets=KeyWrapper(custom_keyset).keyset_list(),
                ),
                target_content_type=Mimetypes.JSON,
                target_uri=None,
                compression=None,
            )
        case PseudoOperation.DEPSEUDONYMIZE:
            return DepseudoFileRequest(
                pseudo_config=PseudoConfig(
                    rules=rules,
                    keysets=KeyWrapper(custom_keyset).keyset_list(),
                ),
                target_content_type=Mimetypes.JSON,
                target_uri=None,
                compression=None,
            )
        case PseudoOperation.REPSEUDONYMIZE:
            if target_rules is not None:
                return RepseudoFileRequest(
                    source_pseudo_config=PseudoConfig(
                        rules=rules,
                        keysets=KeyWrapper(custom_keyset).keyset_list(),
                    ),
                    target_pseudo_config=PseudoConfig(
                        rules=target_rules,
                        keysets=KeyWrapper(target_custom_keyset).keyset_list(),
                    ),
                    target_content_type=Mimetypes.JSON,
                    target_uri=None,
                    compression=None,
                )
            else:
                raise ValueError("Found no target rules")


def get_file_data_from_dataset(
    dataset: FileLikeDatasetDecl | pl.DataFrame,
) -> tuple[BinaryFileDecl, Mimetypes]:
    """Converts the given dataset to a file handle and content type.

    Args:
        dataset (FileLikeDatasetDecl): The provided dataset

    Raises:
        FileNotFoundError: If the file cannot be found
        DefaultCredentialsError: If the provided dataset is a GCS path and no Google credentials are found
        ValueError: If the provided dataset is not of a supported type
        FileInvalidError: If the file is empty

    Returns:
        tuple[BinaryFileDecl, Mimetypes]: A tuple of (file handle, content type)
    """
    file_handle: BinaryFileDecl | None = None
    match dataset:
        case str() | Path():
            # File path
            if str(dataset).startswith("gs://"):
                try:
                    file_handle = FileClient().gcs_open(str(dataset), mode="rb")
                except OSError as err:
                    raise FileNotFoundError(
                        f"No GCS file found or authentication not sufficient for: {dataset}"
                    ) from err
                except DefaultCredentialsError as err:
                    raise DefaultCredentialsError(  # type: ignore[no-untyped-call]
                        "No Google Authentication found in environment"
                    ) from err
            else:
                file_handle = open(dataset, "rb")

            file_handle.seek(0)

        # Convert Polars dataframe to a zipped archive with json data
        case pl.DataFrame() as df:
            file_handle = io.BytesIO()
            with zipfile.ZipFile(
                file_handle, "a", compression=zipfile.ZIP_DEFLATED, compresslevel=9
            ) as zip_file:
                zip_file.writestr("data.json", orjson.dumps(df.to_dicts()))
                zip_file.filename = "data.zip"
            file_handle.seek(0)
            return file_handle, Mimetypes.ZIP

        case io.BufferedReader():
            # File handle
            dataset.seek(0)
            file_handle = dataset
        case fsspec.spec.AbstractBufferedFile():
            # This is a file handle to a remote storage system such as GCS.
            # It provides random access for the underlying file-like data (without downloading the whole thing).
            dataset.seek(0)
            file_handle = io.BufferedReader(dataset)
        case _:
            raise ValueError(
                f"Unsupported data type: {type(dataset)}. Supported types are {FileLikeDatasetDecl}"
            )

    if isinstance(file_handle, GCSFile):
        file_size = file_handle.size
    else:
        file_size = os.fstat(file_handle.fileno()).st_size

    if file_size == 0:
        raise FileInvalidError("File is empty.")

    content_type = get_content_type_from_file(file_handle)
    return file_handle, content_type


def get_content_type_from_file(file_handle: BinaryFileDecl) -> Mimetypes:
    """Given a file handle, extracts the content type from the file.

    Args:
        file_handle (BinaryFileDecl): The file handle to extract the content type from.

    Raises:
        MimetypeNotSupportedError: If the Mimetype is not supported

    Returns:
        Mimetypes: The Mimetype of the file.
    """
    if isinstance(file_handle, GCSFile):
        file_name = file_handle.full_name
    else:
        file_name = file_handle.name
    file_format = get_file_format_from_file_name(file_name)
    try:  # Test whether the file extension is supported
        content_type = Mimetypes(FORMAT_TO_MIMETYPE_FUNCTION[file_format])
    except KeyError:
        raise MimetypeNotSupportedError(
            f"The provided input format '{file_format}' is not supported from file."
        ) from None

    return content_type
