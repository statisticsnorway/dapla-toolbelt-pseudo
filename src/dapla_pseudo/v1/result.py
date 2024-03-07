"""Common API models for builder packages."""

import typing as t
from io import BufferedWriter
from pathlib import Path
from typing import Any

import pandas as pd
import polars as pl
from cloudpathlib import GSClient
from cloudpathlib import GSPath
from dapla import AuthClient
from datadoc_model.model import MetadataContainer
from datadoc_model.model import PseudonymizationMetadata
from datadoc_model.model import PseudoVariable

from dapla_pseudo.utils import get_file_format_from_file_name
from dapla_pseudo.v1.pseudo_commons import PseudoFieldResponse
from dapla_pseudo.v1.pseudo_commons import PseudoFileResponse
from dapla_pseudo.v1.supported_file_format import SupportedOutputFileFormat
from dapla_pseudo.v1.supported_file_format import write_from_df
from dapla_pseudo.v1.supported_file_format import write_from_dicts


class Result:
    """Result represents the result of a pseudonymization operation."""

    def __init__(
        self,
        pseudo_response: PseudoFieldResponse | PseudoFileResponse,
    ) -> None:
        """Initialise a PseudonymizationResult."""
        self._pseudo_data: pl.DataFrame | list[dict[str, t.Any]]
        self._metadata: dict[str, dict[str, list[Any]]] = {}
        match pseudo_response:
            case PseudoFieldResponse(dataframe, raw_metadata):
                self._pseudo_data = dataframe

                datadoc_fields: list[PseudoVariable] = []

                for field_metadata in raw_metadata:
                    pseudo_variable = self._datadoc_from_raw_metadata_fields(
                        field_metadata.datadoc
                    )
                    if pseudo_variable is not None:
                        datadoc_fields.append(pseudo_variable)

                    if field_metadata.field_name is None:
                        field_metadata.field_name = "unknown_field"

                    self._metadata[field_metadata.field_name] = {
                        "logs": field_metadata.logs,
                        "metrics": field_metadata.metrics,
                    }

                self._datadoc = MetadataContainer(
                    pseudonymization=PseudonymizationMetadata(
                        pseudo_variables=datadoc_fields
                    )
                )

            case PseudoFileResponse(
                data, file_metadata, _content_type, file_name, _streamed
            ):
                self._pseudo_data = data
                self._metadata[file_name] = {
                    "logs": file_metadata.logs,
                    "metrics": file_metadata.metrics,
                }
                pseudo_variable = self._datadoc_from_raw_metadata_fields(
                    file_metadata.datadoc
                )
                self._datadoc = MetadataContainer(
                    pseudonymization=PseudonymizationMetadata(
                        pseudo_variables=(
                            [pseudo_variable] if pseudo_variable is not None else []
                        )
                    )
                )

    def to_polars(self, **kwargs: t.Any) -> pl.DataFrame:
        """Output pseudonymized data as a Polars DataFrame.

        Args:
            **kwargs: Additional keyword arguments to be passed the Polars "from_dicts" function *if* the input data is from a file.

        Raises:
            ValueError: If the result is not of type Polars DataFrame or PseudoFileResponse.

        Returns:
            pl.DataFrame: A Polars DataFrame containing the pseudonymized data.
        """
        match self._pseudo_data:
            case pl.DataFrame() as df:
                # Drop statement a workaround to https://github.com/pola-rs/polars/issues/7291
                if "__index_level_0__" in df.columns:
                    df = df.drop("__index_level_0__")
                return df
            case list() as file_data:
                df = pl.from_dicts(file_data, **kwargs)
                return df
            case _ as invalid_pseudo_data:
                raise ValueError(f"Invalid file type: {type(invalid_pseudo_data)}")

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
        match self._pseudo_data:
            case pl.DataFrame() as df:
                return df.to_pandas()
            case list() as file_data:
                return pd.DataFrame.from_records(file_data, **kwargs)
            case _ as invalid_pseudo_data:
                raise ValueError(f"Invalid response type: {type(invalid_pseudo_data)}")

    def to_file(self, file_path: str, **kwargs: t.Any) -> None:
        """Write pseudonymized data to a file, with the metadata being written to the same folder.

        Args:
            file_path (str): The path to the file to be written. If writing to a bucket, use the "gs://" prefix.
            **kwargs: Additional keyword arguments to be passed the Polars writer function *if* the input data is a DataFrame.
                The specific writer function depends on the format of the output file, e.g. `write_csv()` for CSV files.

        Raises:
            ValueError: If the result is not of type Polars DataFrame or PseudoFileResponse.
            ValueError: If the output file format does not match the input file format.

        """
        file_format = get_file_format_from_file_name(file_path)

        datadoc_file_name = f"{Path(file_path).stem}__DOC.json"

        datadoc_file_path: Path | GSPath
        if file_path.startswith(GSPath.cloud_prefix):
            client = GSClient(credentials=AuthClient.fetch_google_credentials())
            gs_path = GSPath(file_path, client)

            file_handle = gs_path.open(mode="wb")

            datadoc_file_path = gs_path.parent.joinpath(Path(datadoc_file_name))
            datadoc_file_handle = datadoc_file_path.open(mode="w")
        else:
            file_handle = Path(file_path).open(mode="wb")

            datadoc_file_path = Path(file_path).parent.joinpath(Path(datadoc_file_name))
            datadoc_file_handle = datadoc_file_path.open(mode="w")

        file_handle = t.cast(
            BufferedWriter, file_handle
        )  # file handle is always BufferedWriter when opening with "wb"

        match self._pseudo_data:
            case pl.DataFrame() as df:
                write_from_df(df, file_format, file_handle, **kwargs)
                datadoc_file_handle.write(self.datadoc)
            case list() as file_data:
                write_from_dicts(
                    file_data, SupportedOutputFileFormat(file_format), file_handle
                )
                datadoc_file_handle.write(self.datadoc)
            case _ as invalid_pseudo_data:
                raise ValueError(f"Invalid response type: {type(invalid_pseudo_data)}")

        file_handle.close()
        datadoc_file_handle.close()

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
