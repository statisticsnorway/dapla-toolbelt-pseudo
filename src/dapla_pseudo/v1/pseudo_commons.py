"""Common functions shared by all pseudo modules."""

import io
import json
import os
import typing as t
from dataclasses import dataclass
from pathlib import Path

import fsspec
import polars as pl
import requests
from dapla import FileClient
from gcsfs.core import GCSFile
from google.auth.exceptions import DefaultCredentialsError

from dapla_pseudo.exceptions import FileInvalidError
from dapla_pseudo.exceptions import MimetypeNotSupportedError
from dapla_pseudo.types import BinaryFileDecl
from dapla_pseudo.types import FileLikeDatasetDecl
from dapla_pseudo.types import FileSpecDecl
from dapla_pseudo.utils import get_file_format_from_file_name
from dapla_pseudo.v1.api_models import DepseudonymizeFileRequest
from dapla_pseudo.v1.api_models import Mimetypes
from dapla_pseudo.v1.api_models import PseudoFunction
from dapla_pseudo.v1.api_models import PseudoKeyset
from dapla_pseudo.v1.api_models import PseudonymizeFileRequest
from dapla_pseudo.v1.api_models import RepseudonymizeFileRequest
from dapla_pseudo.v1.client import PseudoClient
from dapla_pseudo.v1.client import _client
from dapla_pseudo.v1.client import _extract_name
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


@dataclass
class RawPseudoMetadata:
    """RawPseudoMetadata holds the raw metadata obtained from Pseudo Service."""

    logs: list[str]
    metrics: list[str]
    datadoc: list[dict[str, t.Any]]
    field_name: t.Optional[str] = None


@dataclass
class PseudoFieldResponse:
    """PseudoFileResponse holds the data and metadata from a Pseudo Service field response."""

    data: pl.DataFrame
    raw_metadata: list[RawPseudoMetadata]


@dataclass
class PseudoFileResponse:
    """PseudoFileResponse holds the data and metadata from a Pseudo Service file response."""

    data: list[dict[str, t.Any]]
    raw_metadata: RawPseudoMetadata
    content_type: Mimetypes
    file_name: str
    streamed: bool = True


def pseudo_operation_file(
    file_handle: t.BinaryIO,
    pseudo_operation_request: (
        PseudonymizeFileRequest | DepseudonymizeFileRequest | RepseudonymizeFileRequest
    ),
    input_content_type: Mimetypes,
) -> PseudoFileResponse:
    """Makes pseudonymization API calls for a file and returns the pseudonymized data and metadata.

    Args:
        file_handle: A file handle repreresenting the data to be pseudonymized
        pseudo_operation_request: An object representing the data and how it should be pseudonymized
        input_content_type: A supported Mimetype for the file.

    Returns:
        PseudoFileResponse: An object representing the response from the Pseudo Service.
    """
    request_spec: FileSpecDecl = (
        None,
        pseudo_operation_request.to_json(),
        str(Mimetypes.JSON),
    )

    file_name = _extract_name(
        file_handle=file_handle, input_content_type=input_content_type
    )

    data_spec: FileSpecDecl = (
        file_name,
        file_handle,
        str(pseudo_operation_request.target_content_type),
    )

    response = _client()._post_to_file_endpoint(
        path=PseudoClient.pseudo_op_to_endpoint[type(pseudo_operation_request)],
        request_spec=request_spec,
        data_spec=data_spec,
        stream=True,
    )

    file_handle.close()

    payload = json.loads(response.content.decode("utf-8"))
    pseudo_data = payload["data"]
    metadata = RawPseudoMetadata(
        logs=payload["logs"],
        metrics=payload["metrics"],
        datadoc=payload["datadoc_metadata"]["pseudo_variables"],
    )

    return PseudoFileResponse(
        data=pseudo_data,
        raw_metadata=metadata,
        content_type=Mimetypes.JSON,
        streamed=True,
        file_name=file_name,
    )


def pseudonymize_operation_field(
    path: str,
    field_name: str,
    values: list[str],
    pseudo_func: t.Optional[PseudoFunction],
    timeout: int,
    pseudo_client: PseudoClient,
    keyset: t.Optional[PseudoKeyset] = None,
) -> tuple[pl.Series, RawPseudoMetadata]:
    """Makes pseudonymization API calls for a list of values for a specific field and processes it into a polars Series.

    Args:
        path (str): The path to the pseudonymization endpoint.
        field_name (str): The name of the field being pseudonymized.
        values (list[str]): The list of values to be pseudonymized.
        pseudo_func (Optional[PseudoFunction]): The pseudonymization function to apply to the values.
        timeout (int): The timeout in seconds for the API call.
        pseudo_client (PseudoClient): The instance of the pseudo_client used to make http requests.
        keyset (Optional[PseudoKeyset], optional): The pseudonymization keyset to use. Defaults to None.

    Returns:
        pl.Series: A pandas Series containing the pseudonymized values.
    """
    response: requests.Response = pseudo_client._post_to_field_endpoint(
        path,
        field_name,
        values,
        pseudo_func,
        timeout,
        keyset,
        stream=True,
    )
    payload = json.loads(response.content.decode("utf-8"))
    data = payload["data"]
    metadata = RawPseudoMetadata(
        field_name=field_name,
        logs=payload["logs"],
        metrics=payload["metrics"],
        datadoc=payload["datadoc_metadata"]["pseudo_variables"],
    )

    return pl.Series(data), metadata
