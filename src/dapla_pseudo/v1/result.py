"""Common API models for builder packages."""

import typing as t
from io import BytesIO
from pathlib import Path
from typing import Any

import pandas as pd
import polars as pl
from dapla import FileClient
from datadoc_model.model import MetadataContainer
from datadoc_model.model import PseudonymizationJsonSchema
from datadoc_model.model import PseudoVariable

from dapla_pseudo.utils import get_file_format_from_file_name
from dapla_pseudo.v1.pseudo_commons import PseudoFieldResponse
from dapla_pseudo.v1.pseudo_commons import PseudoFileResponse
from dapla_pseudo.v1.supported_file_format import FORMAT_TO_MIMETYPE_FUNCTION
from dapla_pseudo.v1.supported_file_format import SupportedOutputFileFormat
from dapla_pseudo.v1.supported_file_format import read_to_pandas_df
from dapla_pseudo.v1.supported_file_format import read_to_polars_df
from dapla_pseudo.v1.supported_file_format import write_from_df


class Result:
    """Result represents the result of a pseudonymization operation."""

    def __init__(
        self,
        pseudo_response: PseudoFieldResponse | PseudoFileResponse,
    ) -> None:
        """Initialise a PseudonymizationResult."""
        self._pseudo_response = pseudo_response
        match pseudo_response:
            case PseudoFieldResponse(_, raw_metadata):
                datadoc_fields: list[PseudoVariable] = []
                self._metadata: dict[str, dict[str, list[Any]]] = {}

                for field_metadata in raw_metadata:
                    pseudo_variable = self._datadoc_from_raw_metadata_fields(
                        field_metadata.datadoc
                    )
                    if pseudo_variable is not None:
                        datadoc_fields.append(pseudo_variable)

                    self._metadata[field_metadata.field_name] = {
                        "logs": field_metadata.logs,
                        "metrics": field_metadata.metrics,
                    }

                self._datadoc = MetadataContainer(
                    pseudonymization=PseudonymizationJsonSchema(
                        pseudo_variables=datadoc_fields
                    )
                )

            case PseudoFileResponse():
                self._metadata = {}

    def to_polars(self, **kwargs: t.Any) -> pl.DataFrame:
        """Output pseudonymized data as a Polars DataFrame.

        Args:
            **kwargs: Additional keyword arguments to be passed the Polars reader function *if* the input data is from a file.
                The specific reader function depends on the format, e.g. `read_csv` for CSV files.

        Raises:
            ValueError: If the result is not of type Polars DataFrame or PseudoFileResponse.

        Returns:
            pl.DataFrame: A Polars DataFrame containing the pseudonymized data.
        """
        match self._pseudo_response:
            case PseudoFieldResponse():
                # Drop statement a workaround to https://github.com/pola-rs/polars/issues/7291
                return self._pseudo_response.data.drop("__index_level_0__")
            case PseudoFileResponse(response, content_type, _):
                output_format = SupportedOutputFileFormat(content_type.name.lower())
                df = read_to_polars_df(
                    output_format, BytesIO(response.content), **kwargs
                )
                return df
            case _:
                raise ValueError(
                    f"Invalid response type: {type(self._pseudo_response)}"
                )

    def to_pandas(self, **kwargs: t.Any) -> pd.DataFrame:
        """Output pseudonymized data as a Pandas DataFrame.

        Args:
            **kwargs: Additional keyword arguments to be passed the Pandas reader function *if* the input data is from a file.
                The specific reader function depends on the format of the input file, e.g. `read_csv()` for CSV files.

        Raises:
            ValueError: If the result is not of type Polars DataFrame or PseudoFileResponse.

        Returns:
            pd.DataFrame: A Pandas DataFrame containing the pseudonymized data.
        """
        match self._pseudo_response:
            case PseudoFieldResponse(data, _):
                return data.to_pandas()
            case PseudoFileResponse(response, content_type, _):
                output_format = SupportedOutputFileFormat(content_type.name.lower())
                df = read_to_pandas_df(
                    output_format, BytesIO(response.content), **kwargs
                )
                return df
            case _:
                raise ValueError(
                    f"Invalid response type: {type(self._pseudo_response)}"
                )

    def to_file(self, file_path: str | Path, **kwargs: t.Any) -> None:
        """Write pseudonymized data to a file.

        Args:
            file_path (str | Path): The path to the file to be written.
            **kwargs: Additional keyword arguments to be passed the Polars writer function *if* the input data is a DataFrame.
                The specific writer function depends on the format of the output file, e.g. `write_csv()` for CSV files.

        Raises:
            ValueError: If the result is not of type Polars DataFrame or PseudoFileResponse.
            ValueError: If the output file format does not match the input file format.

        """
        file_format = get_file_format_from_file_name(file_path)

        if str(file_path).startswith("gs://"):
            file_handle = FileClient().gcs_open(str(file_path), mode="wb")
        else:
            file_handle = open(file_path, mode="wb")

        match self._pseudo_response:
            case PseudoFileResponse(response, content_type, streamed):
                if FORMAT_TO_MIMETYPE_FUNCTION[file_format] != content_type:
                    raise ValueError(
                        f'Provided output file format "{file_format}" does not'
                        f'match the content type of the provided input file "{content_type.name}".'
                    )
                if streamed:
                    for chunk in response.iter_content(chunk_size=128):
                        file_handle.write(chunk)
                else:
                    # MyPy error below needs to be fixed, ignoring for now
                    file_handle.write(self._pseudo_response.response.content)  # type: ignore[arg-type]
            case pl.DataFrame():
                write_from_df(self._pseudo_response, file_format, file_path, **kwargs)
            case _:
                raise ValueError(
                    f"Invalid response type: {type(self._pseudo_response)}"
                )

        file_handle.close()

    @property
    def metadata(self) -> dict[str, Any]:
        """Returns the pseudonymization metadata as a dictionary.

        Returns:
            Optional[dict[str, str]]: A dictionary containing the pseudonymization metadata,
            where the keys are field names and the values are corresponding pseudo field metadata.
            If no metadata is set, returns an empty dictionary.
        """
        return self._metadata

    @property
    def datadoc(self) -> str:
        """Returns the pseudonymization metadata as a dictionary.

        Returns:
            str: A JSON-formattted string representing the datadoc metadata.
        """
        return self._datadoc.model_dump_json()

    def _datadoc_from_raw_metadata_fields(
        self,
        raw_metadata: list[dict[str, Any]],
    ) -> t.Optional[PseudoVariable]:
        if len(raw_metadata) == 1:  # Only one element in list if NOT using SID-mapping
            return PseudoVariable.model_validate(raw_metadata[0])
        elif len(raw_metadata) == 2 and any(
            "stable_identifier_type" in pseudo_var for pseudo_var in raw_metadata
        ):  # SID-mapping
            sid_metadata = next(
                pseudo_var
                for pseudo_var in raw_metadata
                if "stable_identifier_type" in pseudo_var
            )
            encrypt_metadata = next(
                pseudo_var
                for pseudo_var in raw_metadata
                if "stable_identifier_type" not in pseudo_var
            )
            pseudo_variable = PseudoVariable.model_validate(encrypt_metadata)
            pseudo_variable.stable_identifier_type = sid_metadata[
                "stable_identifier_type"
            ]
            pseudo_variable.stable_identifier_version = sid_metadata[
                "stable_identifier_version"
            ]
            return pseudo_variable
        else:
            return None
