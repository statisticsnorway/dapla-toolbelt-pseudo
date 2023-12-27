"""Builder for submitting a pseudonymization request."""
import concurrent
import io
import json
import os
import typing as t
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Optional

import fsspec
import pandas as pd
import polars as pl
import requests
from dapla import FileClient
from gcsfs.core import GCSFile
from google.auth.exceptions import DefaultCredentialsError
from requests import Response

from dapla_pseudo.constants import TIMEOUT_DEFAULT
from dapla_pseudo.constants import PseudoFunctionTypes
from dapla_pseudo.exceptions import FileInvalidError
from dapla_pseudo.exceptions import MimetypeNotSupportedError
from dapla_pseudo.types import BinaryFileDecl
from dapla_pseudo.types import FileLikeDatasetDecl
from dapla_pseudo.utils import convert_to_date
from dapla_pseudo.utils import get_file_format_from_file_name
from dapla_pseudo.v1.builder_models import PseudoFileResponse
from dapla_pseudo.v1.builder_models import Result
from dapla_pseudo.v1.models import DaeadKeywordArgs
from dapla_pseudo.v1.models import FF31KeywordArgs
from dapla_pseudo.v1.models import KeyWrapper
from dapla_pseudo.v1.models import MapSidKeywordArgs
from dapla_pseudo.v1.models import Mimetypes
from dapla_pseudo.v1.models import PseudoConfig
from dapla_pseudo.v1.models import PseudoFunction
from dapla_pseudo.v1.models import PseudoKeyset
from dapla_pseudo.v1.models import PseudonymizeFileRequest
from dapla_pseudo.v1.models import PseudoRule
from dapla_pseudo.v1.ops import _client
from dapla_pseudo.v1.supported_file_format import FORMAT_TO_MIMETYPE_FUNCTION


@dataclass
class File:
    """File represents a file to be pseudonymized."""

    file_handle: BinaryFileDecl
    content_type: Mimetypes


class PseudoData:
    """Starting point for pseudonymization of datasets.

    This class should not be instantiated, only the static methods should be used.
    """

    dataset: File | pl.DataFrame

    @staticmethod
    def from_pandas(dataframe: pd.DataFrame) -> "PseudoData._Pseudonymizer":
        """Initialize a pseudonymization request from a pandas DataFrame."""
        dataset: pl.DataFrame = pl.from_pandas(dataframe)
        PseudoData.dataset = dataset
        return PseudoData._Pseudonymizer()

    @staticmethod
    def from_polars(dataframe: pl.DataFrame) -> "PseudoData._Pseudonymizer":
        """Initialize a pseudonymization request from a polars DataFrame."""
        PseudoData.dataset = dataframe
        return PseudoData._Pseudonymizer()

    @staticmethod
    def from_file(dataset: FileLikeDatasetDecl) -> "PseudoData._Pseudonymizer":
        """Initialize a pseudonymization request from a pandas dataframe read from file.

        Args:
            dataset (FileLikeDatasetDecl): Either a path to the file to be read, or a file handle.

        Raises:
            FileNotFoundError: If no file is found at the specified path.
            FileInvalidError: If the file is empty.
            ValueError: If the dataset is not of a supported type.
            DefaultCredentialsError: If no Google Authentication is found in the environment.

        Returns:
            _Pseudonymizer: An instance of the _Pseudonymizer class.

        Examples:
            # Read from bucket
            from dapla import AuthClient
            from dapla_pseudo import PseudoData
            bucket_path = "gs://ssb-staging-dapla-felles-data-delt/felles/smoke-tests/fruits/data.parquet"
            field_selector = PseudoData.from_file(bucket_path)

            # Read from local filesystem
            from dapla_pseudo import PseudoData

            local_path = "some_file.csv"
            field_selector = PseudoData.from_file(local_path))
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
                        raise DefaultCredentialsError("No Google Authentication found in environment") from err
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
                raise ValueError(f"Unsupported data type: {type(dataset)}. Supported types are {FileLikeDatasetDecl}")

        if isinstance(file_handle, GCSFile):
            file_size = file_handle.size
        else:
            file_size = os.fstat(file_handle.fileno()).st_size

        if file_size == 0:
            raise FileInvalidError("File is empty.")

        content_type = _get_content_type_from_file(file_handle)
        PseudoData.dataset = File(file_handle, content_type)
        return PseudoData._Pseudonymizer()

    class _Pseudonymizer:
        """Select one or multiple fields to be pseudonymized."""

        def __init__(self, rules: Optional[list[PseudoRule]] = None) -> None:
            """Initialize the class."""
            self._rules: list[PseudoRule] = [] if rules is None else rules
            self._pseudo_keyset: Optional[PseudoKeyset] = None
            self._metadata: t.Dict[str, str] = {}
            self._timeout: int = TIMEOUT_DEFAULT

        def on_fields(self, *fields: str) -> "PseudoData._PseudoFuncSelector":
            """Specify one or multiple fields to be pseudonymized."""
            return PseudoData._PseudoFuncSelector(list(fields), self._rules)

        def pseudonymize(
            self,
            with_custom_keyset: Optional[PseudoKeyset] = None,
            timeout: int = TIMEOUT_DEFAULT,
        ) -> Result:
            """Pseudonymize the dataset.

            Args:
                with_custom_keyset (PseudoKeyset, optional): The pseudonymization keyset to use. Defaults to None.
                timeout (int, optional): The timeout in seconds for the API call. Defaults to TIMEOUT_DEFAULT.
            """
            if PseudoData.dataset is None:
                raise ValueError("No dataset has been provided.")

            if self._rules == []:
                raise ValueError("No fields have been provided. Use the 'on_fields' method.")

            if with_custom_keyset is not None:
                self._pseudo_keyset = with_custom_keyset

            self._timeout = timeout
            match PseudoData.dataset:  # Differentiate between file and DataFrame
                case File():
                    return self._pseudonymize_file()
                case pl.DataFrame():
                    return self._pseudonymize_field()
                case _:
                    raise ValueError(
                        f"Unsupported data type: {type(PseudoData.dataset)}. Should only be DataFrame or file-like type."
                    )

        def _pseudonymize_file(self) -> Result:
            """Pseudonymize the entire file."""
            # Need to type-cast explicitly. We know that PseudoData.dataset is a "File" if we reach this method.
            file = t.cast(File, PseudoData.dataset)

            pseudonymize_request = PseudonymizeFileRequest(
                pseudo_config=PseudoConfig(
                    rules=self._rules,
                    keysets=KeyWrapper(self._pseudo_keyset).keyset_list(),
                ),
                target_content_type=file.content_type,
                target_uri=None,
                compression=None,
            )

            response: Response = _client().pseudonymize_file(
                pseudonymize_request,
                file.file_handle,
                stream=True,
                name=None,
                timeout=self._timeout,
            )
            return Result(
                PseudoFileResponse(response, file.content_type, streamed=True),
                self._metadata,
            )

        def _pseudonymize_field(self) -> Result:
            """Pseudonymizes the specified fields in the DataFrame using the provided pseudonymization function.

            The pseudonymization is performed in parallel. After the parallel processing is finished,
            the pseudonymized fields replace the original fields in the DataFrame stored in `self._dataframe`.

            Returns:
                Result: Containing the pseudonymized 'self._dataframe' and the associated metadata.
            """

            def pseudonymize_field_runner(
                field_name: str, series: pl.Series, pseudo_func: PseudoFunction
            ) -> tuple[str, pl.Series]:
                """Function that performs the pseudonymization on a pandas Series.

                Args:
                    field_name (str):  The name of the field.
                    series (pl.Series): The pandas Series containing the values to be pseudonymized.
                    pseudo_func (PseudoFunction): The pseudonymization function to apply to the values.

                Returns:
                    tuple[str,pl.Series]: A tuple containing the field_name and the corresponding series.
                """
                return (
                    field_name,
                    _do_pseudonymize_field(
                        path="pseudonymize/field",
                        field_name=field_name,
                        values=series.to_list(),
                        pseudo_func=pseudo_func,
                        metadata_map=self._metadata,
                        timeout=self._timeout,
                        keyset=self._pseudo_keyset,
                    ),
                )

            dataframe = t.cast(pd.DataFrame, PseudoData.dataset)
            # Execute the pseudonymization API calls in parallel
            with concurrent.futures.ThreadPoolExecutor() as executor:
                pseudonymized_field: t.Dict[str, pl.Series] = {}
                futures = [
                    executor.submit(
                        pseudonymize_field_runner,
                        rule.pattern,
                        dataframe[rule.pattern],
                        rule.func,
                    )
                    for rule in self._rules
                ]
                # Wait for the futures to finish, then add each field to pseudonymized_field map
                for future in concurrent.futures.as_completed(futures):
                    result = future.result()
                    # Each future result contains the field_name (0) and the pseudonymize_values (1)
                    pseudonymized_field[result[0]] = result[1]

                pseudonymized_df = pl.DataFrame(pseudonymized_field)
                dataframe = dataframe.update(pseudonymized_df)

            return Result(pseudo_response=dataframe, metadata=self._metadata)

    class _PseudoFuncSelector:
        def __init__(self, fields: list[str], rules: Optional[list[PseudoRule]] = None) -> None:
            self._fields = fields
            self._existing_rules = [] if rules is None else rules

        def with_stable_id(self, sid_snapshot_date: Optional[str | date] = None) -> "PseudoData._Pseudonymizer":
            """Map selected fields to stable ID.

            Args:
                sid_snapshot_date (Optional[str | date], optional): Date representing SID-catalogue version to use.
                    Latest if unspecified. Format: YYYY-MM-DD

            Returns:
                Self: The object configured to be mapped to stable ID
            """
            function = PseudoFunction(
                function_type=PseudoFunctionTypes.MAP_SID,
                kwargs=MapSidKeywordArgs(snapshot_date=convert_to_date(sid_snapshot_date)),
            )
            return self._rule_constructor(function)

        def with_default_encryption(self) -> "PseudoData._Pseudonymizer":
            function = PseudoFunction(function_type=PseudoFunctionTypes.DAEAD, kwargs=DaeadKeywordArgs())
            return self._rule_constructor(function)

        def with_papis_compatible_encryption(self) -> "PseudoData._Pseudonymizer":
            function = PseudoFunction(function_type=PseudoFunctionTypes.FF31, kwargs=FF31KeywordArgs())
            return self._rule_constructor(function)

        def with_custom_function(self, function: PseudoFunction) -> "PseudoData._Pseudonymizer":
            return self._rule_constructor(function)

        def _rule_constructor(self, func: PseudoFunction) -> "PseudoData._Pseudonymizer":
            # If we use the pseudonymize_file endpoint, we need a glob catch-all prefix.
            rule_prefix = "**/" if isinstance(PseudoData.dataset, File) else ""
            rules = [PseudoRule(name=None, func=func, pattern=f"{rule_prefix}{field}") for field in self._fields]
            return PseudoData._Pseudonymizer(self._existing_rules + rules)


def _get_content_type_from_file(file_handle: BinaryFileDecl) -> Mimetypes:
    if isinstance(file_handle, GCSFile):
        file_name = file_handle.full_name
    else:
        file_name = file_handle.name
    file_format = get_file_format_from_file_name(file_name)
    try:  # Test whether the file extension is supported
        content_type = Mimetypes(FORMAT_TO_MIMETYPE_FUNCTION[file_format])
    except KeyError:  # Fall back on reading the file format from the magic bytes
        raise MimetypeNotSupportedError(
            f"The provided input format '{file_format}' is not supported from file."
        ) from None

    return content_type


def _do_pseudonymize_field(
    path: str,
    field_name: str,
    values: list[str],
    pseudo_func: Optional[PseudoFunction],
    metadata_map: t.Dict[str, str],
    timeout: int,
    keyset: Optional[PseudoKeyset] = None,
) -> pl.Series:
    """Makes pseudonymization API calls for a list of values for a specific field and processes it into a polars Series.

    Args:
        path (str): The path to the pseudonymization endpoint.
        field_name (str): The name of the field being pseudonymized.
        values (list[str]): The list of values to be pseudonymized.
        pseudo_func (Optional[PseudoFunction]): The pseudonymization function to apply to the values.
        metadata_map (Dict[str, str]): A dictionary to store the metadata associated with each field.
        timeout (int): The timeout in seconds for the API call.
        keyset (Optional[PseudoKeyset], optional): The pseudonymization keyset to use. Defaults to None.

    Returns:
        pl.Series: A pandas Series containing the pseudonymized values.
    """
    response: requests.Response = _client()._post_to_field_endpoint(
        path, field_name, values, pseudo_func, timeout, keyset, stream=True
    )
    metadata_map[field_name] = str(response.headers.get("metadata") or "")

    # The response content is received as a buffered byte stream from the server.
    # We decode the content using UTF-8, which gives us a List[List[str]] structure.
    # To obtain a single list of strings, we combine the values from the nested sublists into a flat list.
    nested_list = json.loads(response.content.decode("utf-8"))
    combined_list = []
    for sublist in nested_list:
        combined_list.extend(sublist)

    return pl.Series(combined_list)
