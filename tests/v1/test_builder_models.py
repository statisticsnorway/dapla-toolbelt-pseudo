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
from typeguard import suppress_type_checks

from dapla_pseudo.v1.builder_models import PseudoFileResponse
from dapla_pseudo.v1.builder_models import Result
from dapla_pseudo.v1.models import Mimetypes


TEST_FILE_PATH = "tests/v1/test_files"


@pytest.fixture()
def polars_df() -> pl.DataFrame:
    with open("tests/data/personer.json") as test_data:
        return pl.from_pandas(pd.json_normalize(json.load(test_data)))


@pytest.fixture(
    params=list(product([True, False], Mimetypes.__members__.values()))
)  # tabulate every combination of "streamed" and "content_type"
def pseudo_file_response(request: pytest.FixtureRequest) -> PseudoFileResponse:
    def get_byte_iterator(file_handle: BufferedReader) -> Iterator[bytes]:
        while True:
            chunk = file_handle.read(128)
            if not chunk:  # Last chunk = empty string
                break
            yield chunk

    streamed, content_type = request.param

    fd = open(f"{TEST_FILE_PATH}/test.{content_type.split('/')[-1]}", "rb")
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


def test_result_from_file_to_file(tmp_path: Path, pseudo_file_response: PseudoFileResponse) -> None:
    result = Result(pseudo_response=pseudo_file_response)
    file_extension = pseudo_file_response.content_type.name.lower()
    result.to_file(tmp_path / f"file_to_file.{file_extension}")


@suppress_type_checks  # type: ignore [misc]
def test_result_to_pandas_invalid_type() -> None:
    result = Result(pseudo_response="not a DataFrame")  # type: ignore [arg-type]
    with pytest.raises(ValueError):
        result.to_pandas()


@suppress_type_checks  # type: ignore [misc]
def test_result_to_polars_invalid_type() -> None:
    result = Result(pseudo_response="not a DataFrame")  # type: ignore [arg-type]
    with pytest.raises(ValueError):
        result.to_polars()


@suppress_type_checks  # type: ignore [misc]
def test_result_to_file_invalid_type(tmp_path: Path) -> None:
    result = Result(pseudo_response="not a file")  # type: ignore [arg-type]
    with pytest.raises(ValueError):
        result.to_file(tmp_path / "invalid_type.json")


def test_result_different_file_format(tmp_path: Path) -> None:
    response = Mock(spec=Response)
    result = Result(pseudo_response=PseudoFileResponse(response, Mimetypes.JSON, streamed=True))
    with pytest.raises(ValueError):
        result.to_file(tmp_path / "invalid_file_format.csv")
