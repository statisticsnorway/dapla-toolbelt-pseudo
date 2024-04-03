import io
import json
from pathlib import Path

import pandas as pd
import polars as pl
import pytest

from dapla_pseudo.v1.models.api import PseudoFieldResponse
from dapla_pseudo.v1.models.api import PseudoFileResponse
from dapla_pseudo.v1.models.api import RawPseudoMetadata
from dapla_pseudo.v1.models.core import Mimetypes
from dapla_pseudo.v1.result import Result


@pytest.fixture()
def pseudo_file_response() -> PseudoFileResponse:
    fd = open("tests/data/personer.json")
    data = json.loads(fd.read())
    fd.seek(0)

    return PseudoFileResponse(
        data=data,
        raw_metadata=RawPseudoMetadata(
            logs=[], metrics=[], datadoc=[], field_name="test"
        ),
        content_type=Mimetypes.JSON,
        file_name="personer.json",
    )


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


def test_result_from_polars_to_pandas(df_personer: pl.DataFrame) -> None:
    result = Result(PseudoFieldResponse(data=df_personer, raw_metadata=[]))
    assert isinstance(result.to_pandas(), pd.DataFrame)


def test_result_from_polars_to_file(tmp_path: Path, df_personer: pl.DataFrame) -> None:
    result = Result(PseudoFieldResponse(data=df_personer, raw_metadata=[]))
    result.to_file(str(tmp_path / "polars_to_file.json"))


def test_result_from_file_to_polars(pseudo_file_response: PseudoFileResponse) -> None:
    result = Result(pseudo_response=pseudo_file_response)
    assert isinstance(result.to_polars(), pl.DataFrame)


def test_result_from_file_to_pandas(pseudo_file_response: PseudoFileResponse) -> None:
    result = Result(pseudo_response=pseudo_file_response)
    assert isinstance(result.to_pandas(), pd.DataFrame)


def test_result_from_file_to_file(
    tmp_path: Path, pseudo_file_response: PseudoFileResponse
) -> None:
    result = Result(pseudo_response=pseudo_file_response)
    file_extension = pseudo_file_response.content_type.name.lower()
    result.to_file(str(tmp_path / f"file_to_file.{file_extension}"))
