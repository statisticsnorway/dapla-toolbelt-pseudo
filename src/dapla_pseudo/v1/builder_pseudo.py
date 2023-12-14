"""Builder for submitting a pseudonymization request."""
import concurrent
import io
import json
import os
import typing as t
from datetime import date
from pathlib import Path
from typing import Any
from typing import Optional

import fsspec


# isort: off
import pylibmagic  # noqa Must be imported before magic

# isort: on
import magic
import pandas as pd
import polars as pl
import requests
from requests import Response
from typing_extensions import Self

from dapla_pseudo.constants import PredefinedKeys
from dapla_pseudo.constants import PseudoFunctionTypes
from dapla_pseudo.constants import TIMEOUT_DEFAULT
from dapla_pseudo.types import BinaryFileDecl, HierarchDatasetDecl
from dapla_pseudo.types import DatasetDecl
from dapla_pseudo.types import FieldDecl
from dapla_pseudo.utils import convert_to_date
from dapla_pseudo.v1.builder_models import DataFrameResult, FileResult
from dapla_pseudo.v1.models import DaeadKeywordArgs
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
from dapla_pseudo.v1.ops import _client
from dapla_pseudo.v1.ops import _dataframe_to_json
from dapla_pseudo.v1.supported_file_format import FORMAT_TO_MIMETYPE_FUNCTION, NoFileExtensionError
from dapla_pseudo.v1.supported_file_format import SupportedFileFormat
from dapla_pseudo.v1.supported_file_format import read_to_df


class PseudoData:
    """Starting point for pseudonymization of datasets.

    This class should not be instantiated, only the static methods should be used.
    """

    dataset: t.Union[io.BufferedReader, pl.DataFrame]

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
    def from_file(dataset: DatasetDecl) -> "PseudoData._Pseudonymizer":
        """Initialize a pseudonymization request from a pandas dataframe read from file.

        Args:
            file_path_str (str): The path to the file to be read.
            kwargs (dict): Additional keyword arguments to be passed to the file reader.

        Raises:
            FileNotFoundError: If no file is found at the specified local path.
            NoFileExtensionError: If the file has no extension.

        Returns:
            _FieldSelector: An instance of the _FieldSelector class.

        Examples:
            # Read from bucket
            from dapla import AuthClient
            from dapla_pseudo import PseudoData
            bucket_path = "gs://ssb-staging-dapla-felles-data-delt/felles/smoke-tests/fruits/data.parquet"
            storage_options = {"token": AuthClient.fetch_google_credentials()}
            field_selector = PseudoData.from_file(bucket_path, storage_options=storage_options)

            # Read from local filesystem
            from dapla_pseudo import PseudoData

            local_path = "some_file.csv"
            field_selector = PseudoData.from_file(local_path)
        """
        file_handle: t.Optional[BinaryFileDecl] = None
        match dataset:
            case str() | Path():
                # File path
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
                raise ValueError(f"Unsupported data type: {type(dataset)}. Supported types are {HierarchDatasetDecl}")

        PseudoData.dataset = file_handle
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
            """Specify multiple fields to be pseudonymized."""
            return PseudoData._PseudoFuncSelector(list(fields), self._rules)

        def pseudonymize(
            self, with_custom_keyset: Optional[PseudoKeyset] = None, timeout: int = TIMEOUT_DEFAULT
        ) -> DataFrameResult | FileResult:
            """Pseudonymize the entire dataset."""
            if PseudoData.dataset is None:
                raise ValueError("No dataset has been provided.")

            if self._rules == []:
                raise ValueError("No fields have been provided. Use the 'on_fields' method.")

            if with_custom_keyset is not None:
                self._pseudo_keyset = with_custom_keyset

            self._timeout = timeout
            match PseudoData.dataset:  # Differentiate between hierarchical and tabular datasets
                case io.BufferedReader():
                    return self._pseudonymize_file()
                case pl.DataFrame():
                    return self._pseudonymize_field()
                case _:
                    raise ValueError(
                        f"Unsupported data type: {type(PseudoData.dataset)}. Should only be DataFrame or BufferedReader."
                    )

        def _pseudonymize_file(self) -> FileResult:
            """Pseudonymize the entire file."""
            # Need to type-cast explicitly. We know that PseudoData.dataset is a BufferedReader if we reach this method.
            file_handle = t.cast(io.BufferedReader, PseudoData.dataset)

            try:
                file_path = Path(file_handle.name)
                file_extension = file_path.suffix[1:]
                file_format = SupportedFileFormat(file_extension)
            except ValueError:
                raise NoFileExtensionError(f"File '{file_path}' has no file extension.")

            try:  # Test whether the file extension is supported
                content_type = Mimetypes(FORMAT_TO_MIMETYPE_FUNCTION[file_format])
            except KeyError:  # Fall back on reading the file format from the magic bytes
                magic_content_type = magic.from_buffer(file_handle.read(2048), mime=True)
                # Reset the file pointer to the beginning of the file after reading from magic bytes
                file_handle.seek(0)
                if isinstance(magic_content_type, Mimetypes):
                    content_type = Mimetypes(magic_content_type)
                else:
                    raise ValueError(f"Unsupported file format: {magic_content_type}")

            pseudonymize_request = PseudonymizeFileRequest(
                pseudo_config=PseudoConfig(rules=self._rules, keysets=KeyWrapper(self._pseudo_keyset).keyset_list()),
                target_content_type=content_type,
                target_uri=None,
                compression=None,
            )

            response: Response = _client().pseudonymize_file(
                pseudonymize_request, file_handle, stream=True, name=None, timeout=self._timeout
            )
            return FileResult(response, self._metadata, streamed=True)

        def _pseudonymize_field(self) -> "DataFrameResult":
            """Pseudonymizes the specified fields in the DataFrame using the provided pseudonymization function.

            The pseudonymization is performed in parallel. After the parallel processing is finished,
            the pseudonymized fields replace the original fields in the DataFrame stored in `self._dataframe`.

            Returns:
                DataFrameResult: Containing the pseudonymized 'self._dataframe' and the associated metadata.
            """

            def pseudonymize_field_runner(
                field_name: str, series: pl.Series, pseudo_func: PseudoFunction
            ) -> tuple[str, pl.Series]:
                """Function that performs the pseudonymization on a pandas Series.

                Args:
                    series (pl.Series): The pandas Series containing the values to be pseudonymized.
                    field_name (str):  The name of the field.

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

            dataframe: pd.DataFrame = PseudoData.dataset
            # Execute the pseudonymization API calls in parallel
            with concurrent.futures.ThreadPoolExecutor() as executor:
                pseudonymized_field: t.Dict[str, pl.Series] = {}
                futures = [
                    executor.submit(pseudonymize_field_runner, rule.pattern, dataframe[rule.pattern], rule.func)
                    for rule in self._rules
                ]
                # Wait for the futures to finish, then add each field to pseudonymized_field map
                for future in concurrent.futures.as_completed(futures):
                    result = future.result()
                    # Each future result contains the field_name (0) and the pseudonymize_values (1)
                    pseudonymized_field[result[0]] = result[1]

                pseudonymized_df = pl.DataFrame(pseudonymized_field)
                dataframe = dataframe.update(pseudonymized_df)

            return DataFrameResult(df=dataframe, metadata=self._metadata)

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
            rules = [PseudoRule(name=None, func=func, pattern=f"**/{field}") for field in self._fields]
            return PseudoData._Pseudonymizer(self._existing_rules + rules)


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
