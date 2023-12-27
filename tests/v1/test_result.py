import json
from io import BufferedReader
from itertools import product
from pathlib import Path
from typing import Iterator
from unittest.mock import Mock

import pandas as pd
import polars as pl
import pytest
from requests import Response

from dapla_pseudo.v1.builder_models import PseudoFileResponse
from dapla_pseudo.v1.builder_models import Result
from dapla_pseudo.v1.models import Mimetypes


@pytest.fixture()
def polars_df() -> pl.DataFrame:
    with open("tests/data/personer.json") as test_data:
        return pl.from_pandas(pd.json_normalize(json.load(test_data)))


@pytest.fixture(
    params=list(product([True, False], Mimetypes.__members__.values()))
)  # tabulate every combination of "streamed" and "content_type"
def pseudo_file_response(request: pytest.FixtureRequest) -> PseudoFileResponse:
    def get_byte_iterator(file_handle: BufferedReader) -> Iterator[bytes]:
        print(file_handle.closed)
        while True:
            chunk = file_handle.read(128)
            if not chunk:  # Last chunk = empty string
                break
            yield chunk

    streamed, content_type = request.param

    fd = open("tests/data/personer.json", "rb")
    content = fd.read()
    fd.seek(0)
    content_iterator = get_byte_iterator(fd)

    response_mock = Mock(spec=Response)
    response_mock.content = content
    response_mock.iter_content.return_value = content_iterator
    response_mock.text = content.decode("utf-8")

    return PseudoFileResponse(response_mock, content_type, streamed)


def test_result_from_polars_to_polars(polars_df: pl.DataFrame) -> None:
    result = Result(pseudo_response=polars_df)
    assert isinstance(result.to_polars(), pl.DataFrame)


def test_result_from_polars_to_pandas(polars_df: pl.DataFrame) -> None:
    result = Result(pseudo_response=polars_df)
    assert isinstance(result.to_pandas(), pd.DataFrame)


def test_result_from_polars_to_file(tmp_path: Path, polars_df: pl.DataFrame) -> None:
    result = Result(pseudo_response=polars_df)
    result.to_file(tmp_path / "polars_to_file.json")


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
    result.to_file(tmp_path / f"file_to_file.{file_extension}")
