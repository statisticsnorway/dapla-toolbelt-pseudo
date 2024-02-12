from dapla_pseudo import Validator
from tests.integration.utils import integration_test


@integration_test()
def test_validate(df_personer, setup) -> None:
    result = (
        Validator.from_pandas(df_personer).on_field("fnr").validate_map_to_stable_id()
    )
    print(result.to_polars())


@integration_test()
def test_validate_not_valid(df_personer, setup) -> None:
    result = (
        Validator.from_pandas(df_personer).on_field("fnr").validate_map_to_stable_id()
    )
    print(result.to_polars())
