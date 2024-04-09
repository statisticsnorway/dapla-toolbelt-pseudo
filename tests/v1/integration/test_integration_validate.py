import polars as pl
import pytest

from dapla_pseudo import Validator
from tests.v1.integration.utils import integration_test


@pytest.mark.usefixtures("setup")
@integration_test()
def test_validate(df_personer: pl.DataFrame) -> None:
    result = (
        Validator.from_polars(df_personer).on_field("fnr").validate_map_to_stable_id()
    )
    pl_result = result.to_polars()
    assert pl_result["fnr"].to_list() == []


@pytest.mark.usefixtures("setup")
@integration_test()
def test_validate_not_valid(df_personer: pl.DataFrame) -> None:
    expected_result = ["00000000000", "11111111111"]
    new_data = pl.DataFrame({"fnr": ["11854898347", *expected_result]})

    df_personer = df_personer.update(new_data)

    result = (
        Validator.from_polars(df_personer).on_field("fnr").validate_map_to_stable_id()
    )
    pl_result = result.to_polars()
    assert sorted(pl_result["fnr"].to_list()) == sorted(expected_result)
