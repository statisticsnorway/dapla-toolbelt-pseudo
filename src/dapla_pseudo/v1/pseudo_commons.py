"""Common functions shared by all pseudo modules."""
import io
import os
import typing as t
from dataclasses import dataclass
from pathlib import Path

import fsspec
from dapla import FileClient
from gcsfs.core import GCSFile
from google.auth.exceptions import DefaultCredentialsError

from dapla_pseudo.exceptions import FileInvalidError
from dapla_pseudo.exceptions import MimetypeNotSupportedError
from dapla_pseudo.types import BinaryFileDecl
from dapla_pseudo.types import FileLikeDatasetDecl
from dapla_pseudo.utils import get_file_format_from_file_name
from dapla_pseudo.v1.api_models import Mimetypes
from dapla_pseudo.v1.supported_file_format import FORMAT_TO_MIMETYPE_FUNCTION


@dataclass
class File:
    """'File' represents a file to be pseudonymized."""

    file_handle: BinaryFileDecl
    content_type: Mimetypes


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
                    file_handle = FileClient().gcs_open(dataset, mode="rb")
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
