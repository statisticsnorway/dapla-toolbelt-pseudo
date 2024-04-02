"""Utility functions for Dapla Pseudo."""

import io
import os
import typing as t
from datetime import date
from pathlib import Path

import fsspec
from dapla import FileClient
from gcsfs.core import GCSFile
from google.auth.exceptions import DefaultCredentialsError

from dapla_pseudo.exceptions import FileInvalidError
from dapla_pseudo.exceptions import MimetypeNotSupportedError
from dapla_pseudo.exceptions import NoFileExtensionError
from dapla_pseudo.types import BinaryFileDecl
from dapla_pseudo.types import FileLikeDatasetDecl
from dapla_pseudo.v1.models.api import Mimetypes
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


def convert_to_date(sid_snapshot_date: t.Optional[date | str]) -> t.Optional[date]:
    """Converts the SID version date to the 'date' type."""
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


def get_file_data_from_dataset(
    dataset: FileLikeDatasetDecl,
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
    file_handle: t.Optional[BinaryFileDecl] = None
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
                    raise DefaultCredentialsError(
                        "No Google Authentication found in environment"
                    ) from err
            else:
                file_handle = open(dataset, "rb")

            file_handle.seek(0)

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
