from collections.abc import Generator

import polars as pl

from dapla_pseudo import Validator
from tests.integration.utils import df_personer
from tests.integration.utils import integration_test
from tests.integration.utils import setup


@integration_test()
def test_validate(
    df_personer: pl.DataFrame, setup: Generator[None, None, None]
) -> None:
    result = (
        Validator.from_polars(df_personer).on_field("fnr").validate_map_to_stable_id()
    )
    pl_result = result.to_polars()
    assert pl_result["fnr"].to_list() == []


@integration_test()
def test_validate_not_valid(
    df_personer: pl.DataFrame, setup: Generator[None, None, None]
) -> None:
    expected_result = ["00000000000", "11111111111"]
    new_data = pl.DataFrame({"fnr": ["11854898347", *expected_result]})

    df_personer = df_personer.update(new_data)

    result = (
        Validator.from_polars(df_personer).on_field("fnr").validate_map_to_stable_id()
    )
    pl_result = result.to_polars()
    assert sorted(pl_result["fnr"].to_list()) == sorted(expected_result)
