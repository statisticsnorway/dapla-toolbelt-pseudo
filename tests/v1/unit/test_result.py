import io
import json
from pathlib import Path
from typing import Any

import pandas as pd
import polars as pl

from dapla_pseudo.v1.models.api import PseudoFieldResponse
from dapla_pseudo.v1.models.api import RawPseudoMetadata
from dapla_pseudo.v1.result import Result
from dapla_pseudo.v1.result import aggregate_metrics


def test_result_index_level(tmp_path: Path) -> None:
    # Related to https://github.com/pola-rs/polars/issues/7291
    # If this test fails, hopefully this means that the issue is fixed
    # and the test can be removed, as well as the code in Result
    # removing the column "__index_level_0__"

    df = pd.read_csv(
        io.StringIO(
            """
            a	b
            1	4
            2	5
            3	6
        """
        ),
        sep="\t",
    )

    path_filtered = f"{tmp_path}/filtered.parquet"
    df.query("b % 2 == 0").to_parquet(path_filtered, engine="pyarrow")
    df_pl_filtered = pl.read_parquet(path_filtered)
    assert "__index_level_0__" in df_pl_filtered.columns

    df_result = Result(
        PseudoFieldResponse(data=df_pl_filtered, raw_metadata=[])
    ).to_polars()
    assert "__index_level_0__" not in df_result.columns


def test_result_from_polars_to_polars(df_personer: pl.DataFrame) -> None:
    result = Result(PseudoFieldResponse(data=df_personer, raw_metadata=[]))
    assert isinstance(result.to_polars(), pl.DataFrame)


def test_result_old_datadoc_metadata(df_personer: pl.DataFrame) -> None:
    """Old datadoc metadata is 'supported' in the client.

    Ensure that old metadata doesn't crash the client, but instead
    returns an empty metadata object.
    """
    mocked_metadata = RawPseudoMetadata(
        field_name="fnr",
        logs=[],
        metrics=[{"MAPPED_SID": 3}],
        datadoc=[
            {
                "short_name": "fnr",
                "data_element_path": "fnr",
                "data_element_pattern": "fnr*",
                "stable_identifier_type": "FREG_SNR",
                "stable_identifier_version": "2023-08-31",
                "encryption_algorithm": "TINK-FPE",
                "encryption_key_reference": "papis-common-key-1",
                "encryption_algorithm_parameters": [
                    {"keyId": "papis-common-key-1"},
                    {"strategy": "skip"},
                ],
            }
        ],
    )
    result = Result(
        PseudoFieldResponse(data=df_personer, raw_metadata=[mocked_metadata])
    )
    expected_datadoc = {
        "document_version": "1.0.0",
        "datadoc": {"document_version": "5.0.1", "variables": []},
    }
    assert json.loads(result.datadoc) == expected_datadoc


def test_result_datadoc_metadata(df_personer: pl.DataFrame) -> None:
    """New datadoc metadata is supported in the client.

    Ensure that new metadata is validated correctly by the client
    without errors.
    """
    mocked_metadata = RawPseudoMetadata(
        field_name="fnr",
        logs=[],
        metrics=[{"MAPPED_SID": 3}],
        datadoc=[
            {
                "short_name": "fnr",
                "data_element_path": "fnr",
                "pseudonymization": {
                    "stable_identifier_type": "FREG_SNR",
                    "stable_identifier_version": "2023-08-31",
                    "encryption_algorithm": "TINK-FPE",
                    "encryption_key_reference": "papis-common-key-1",
                    "encryption_algorithm_parameters": [
                        {"keyId": "papis-common-key-1"},
                        {"strategy": "skip"},
                    ],
                },
            }
        ],
    )
    result = Result(
        PseudoFieldResponse(data=df_personer, raw_metadata=[mocked_metadata])
    )
    expected_datadoc = {
        "document_version": "1.0.0",
        "datadoc": {
            "document_version": "5.0.1",
            "variables": mocked_metadata.datadoc,
        },
    }
    assert json.loads(result.datadoc) == expected_datadoc


def test_result_from_polars_to_pandas(df_personer: pl.DataFrame) -> None:
    result = Result(PseudoFieldResponse(data=df_personer, raw_metadata=[]))
    assert isinstance(result.to_pandas(), pd.DataFrame)


def test_result_from_polars_to_file(tmp_path: Path, df_personer: pl.DataFrame) -> None:
    result = Result(PseudoFieldResponse(data=df_personer, raw_metadata=[]))
    result.to_file(str(tmp_path / "polars_to_file.json"))


def test_aggregate_single_metric() -> None:
    field_metadata: dict[str, dict[str, list[Any]]] = {
        "field:": {
            "logs": ["Some log"],
            "metrics": [{"METRIC_1": 1}],
        }
    }
    aggregated_metrics = aggregate_metrics(field_metadata)
    assert aggregated_metrics == {"logs": ["Some log"], "metrics": {"METRIC_1": 1}}


def test_aggregate_same_metrics() -> None:
    field_metadata: dict[str, dict[str, list[Any]]] = {
        "field-1:": {
            "logs": ["Some log"],
            "metrics": [{"METRIC_1": 1}],
        },
        "field-2:": {
            "logs": ["Some log"],
            "metrics": [{"METRIC_1": 2}],
        },
    }
    aggregated_metrics = aggregate_metrics(field_metadata)
    assert aggregated_metrics == {
        "logs": ["Some log", "Some log"],
        "metrics": {"METRIC_1": 3},
    }


def test_aggregate_mixed_metrics() -> None:
    field_metadata: dict[str, dict[str, list[Any]]] = {
        "field-1:": {
            "logs": ["Some log"],
            "metrics": [{"METRIC_1": 2}],
        },
        "field-2:": {
            "logs": ["Some log"],
            "metrics": [{"METRIC_1": 1}],
        },
        "field-3:": {
            "logs": ["Some other log"],
            "metrics": [{"METRIC_2": 3}],
        },
    }
    aggregated_metrics = aggregate_metrics(field_metadata)
    assert aggregated_metrics == {
        "logs": ["Some log", "Some log", "Some other log"],
        "metrics": {"METRIC_1": 3, "METRIC_2": 3},
    }
