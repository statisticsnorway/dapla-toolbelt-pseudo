"""Common API models for builder packages."""

from collections import Counter
from pathlib import Path
from typing import Any
from typing import cast

import pandas as pd
import polars as pl
from cloudpathlib import GSClient
from cloudpathlib import GSPath
from dapla import AuthClient
from datadoc_model.all_optional.model import DatadocMetadata
from datadoc_model.all_optional.model import MetadataContainer
from datadoc_model.all_optional.model import Variable
from pydantic import ValidationError

from dapla_pseudo.utils import get_file_format_from_file_name
from dapla_pseudo.v1.models.api import PseudoFieldResponse
from dapla_pseudo.v1.supported_file_format import write_from_df


class Result:
    """Result represents the result of a pseudonymization operation."""

    def __init__(
        self,
        pseudo_response: PseudoFieldResponse,
    ) -> None:
        """Initialise a PseudonymizationResult."""
        self._pseudo_data: pl.DataFrame = pseudo_response.data
        self._metadata: dict[str, dict[str, list[Any]]] = {}
        self._datadoc: MetadataContainer

        datadoc_fields: list[Variable] = []
        datadoc_paths: list[str | None] = []

        for field_metadata in pseudo_response.raw_metadata:
            pseudo_variable: Variable | None = self._datadoc_from_raw_metadata_fields(
                field_metadata.datadoc
            )
            if (
                pseudo_variable is not None
                and pseudo_variable.data_element_path not in datadoc_paths
            ):
                datadoc_paths.append(pseudo_variable.data_element_path)
                datadoc_fields.append(pseudo_variable)

            if field_metadata.field_name is None:
                field_metadata.field_name = "unknown_field"

            # Add metadata per field
            self._metadata[field_metadata.field_name] = {
                "logs": field_metadata.logs,
                "metrics": field_metadata.metrics,
            }

        self._datadoc = MetadataContainer(
            datadoc=DatadocMetadata(
                document_version="5.0.1",
                variables=datadoc_fields,
            )
        )

    def to_polars(self, **kwargs: Any) -> pl.DataFrame:
        """Output pseudonymized data as a Polars DataFrame.

        Args:
            **kwargs: Additional keyword arguments to be passed the Polars "from_dicts" function *if* the input data is from a file.

        Raises:
            ValueError: If the result is not of type Polars DataFrame.

        Returns:
            pl.DataFrame: A Polars DataFrame containing the pseudonymized data.
        """
        match self._pseudo_data:
            case pl.DataFrame() as df:
                # Drop statement a workaround to https://github.com/pola-rs/polars/issues/7291
                if "__index_level_0__" in df.columns:
                    df = df.drop("__index_level_0__")
                return df
            case _ as invalid_pseudo_data:
                raise ValueError(f"Invalid file type: {type(invalid_pseudo_data)}")

    def to_pandas(self, **kwargs: Any) -> pd.DataFrame:
        """Output pseudonymized data as a Pandas DataFrame.

        Args:
            **kwargs: Additional keyword arguments to be passed the Pandas reader function *if* the input data is from a file.
                The specific reader function depends on the format of the input file, e.g. `read_csv()` for CSV files.

        Raises:
            ValueError: If the result is not of type Polars DataFrame.

        Returns:
            pd.DataFrame: A Pandas DataFrame containing the pseudonymized data.
        """
        match self._pseudo_data:
            case pl.DataFrame() as df:
                return df.to_pandas()
            case _ as invalid_pseudo_data:
                raise ValueError(f"Invalid response type: {type(invalid_pseudo_data)}")

    def to_file(self, file_path: str, **kwargs: Any) -> None:
        """Write pseudonymized data to a file, with the metadata being written to the same folder.

        Args:
            file_path (str): The path to the file to be written. If writing to a bucket, use the "gs://" prefix.
            **kwargs: Additional keyword arguments to be passed the Polars writer function *if* the input data is a DataFrame.
                The specific writer function depends on the format of the output file, e.g. `write_csv()` for CSV files.

        Raises:
            ValueError: If the result is not of type Polars DataFrame.
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

        match self._pseudo_data:
            case pl.DataFrame() as df:
                write_from_df(df, file_format, file_handle, **kwargs)
                datadoc_file_handle.write(self.datadoc)
            case _ as invalid_pseudo_data:
                raise ValueError(f"Invalid response type: {type(invalid_pseudo_data)}")

        file_handle.close()
        datadoc_file_handle.close()

    @property
    def metadata_details(self) -> dict[str, Any]:
        """Returns the pseudonymization metadata as a dictionary, for each field that has been processed.

        Returns:
            Optional[dict[str, str]]: A dictionary containing the pseudonymization metadata,
            where the keys are field names and the values are corresponding pseudo field metadata.
            If no metadata is set, returns an empty dictionary.
        """
        return self._metadata

    @property
    def metadata(self) -> dict[str, Any]:
        """Returns the aggregated metadata for all fields as a dictionary.

        Returns:
            Optional[dict[str, str]]: A dictionary containing the pseudonymization metadata,
            where the keys are field names and the values are corresponding pseudo field metadata.
            If no metadata is set, returns an empty dictionary.
        """
        return aggregate_metrics(self._metadata)

    @property
    def datadoc(self) -> str:
        """Returns the pseudonymization metadata as a dictionary.

        Returns:
            str: A JSON-formattted string representing the datadoc metadata.
        """
        return self._datadoc.model_dump_json(exclude_none=True)

    def _datadoc_from_raw_metadata_fields(
        self,
        raw_metadata: list[dict[str, Any]] | None,
    ) -> Variable | None:

        # This happens if we recieve old datadoc metadata from pseudo-service
        if raw_metadata is None:
            print(
                """[WARNING] 'variables' key not found in datadoc metadata recieved from pseudo-service. This means the metadata is from an outdated datadoc model."""
            )
            return None
        elif len(raw_metadata) == 0:
            return None
        elif len(raw_metadata) > 1:
            print(f"Unexpected length of metadata: {len(raw_metadata)}")

        try:
            variable = Variable(**raw_metadata[0])
            return Variable.model_validate(variable)
        except ValidationError as exc:
            print(
                f"""[WARNING] Datadoc 'Variable' validation failed:
                {exc}"""
            )
            return None


def aggregate_metrics(metadata: dict[str, dict[str, list[Any]]]) -> dict[str, Any]:
    """Aggregates logs and metrics. Each unique metric is summarized."""
    logs: list[str] = []
    metrics: Counter[str] = Counter()
    for field_metadata in metadata.values():
        for metadata_field, metadata_value in field_metadata.items():
            match metadata_field:
                case "logs":
                    # Logs are simply appended
                    logs.extend(metadata_value)
                case "metrics":
                    # Count each unique metric
                    metadata_value = cast(list[dict[str, int]], metadata_value)
                    # Metrics is represented by a list of dicts, e.g. [{"METRIC_1": 1}, {"METRIC_2": 1}]
                    for metric in metadata_value:
                        # Each dict has a single entry, e.g. {"METRIC_1": 1}, so take the first one
                        key, value = next(iter(metric.items()))
                        metrics.update({key: value})
    return {"logs": logs, "metrics": dict(metrics)}
