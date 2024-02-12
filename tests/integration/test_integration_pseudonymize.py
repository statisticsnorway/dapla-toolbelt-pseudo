from collections.abc import Generator

import polars as pl

from dapla_pseudo import Pseudonymize
from tests.integration.utils import integration_test


@integration_test()
def test_pseudonymize_default_encryption(
    setup: Generator[None, None, None], df_personer: pl.DataFrame
) -> None:
    expected_result_fnr_df = pl.DataFrame(
        {
            "fnr": [
                "AWIRfKLSNfR0ID+wBzogEcUT7JQPayk7Gosij6SXr8s=",
                "AWIRfKKLagk0LqYCKpiC4xfPkHqIWGVfc3wg5gUwRNE=",
                "AWIRfKIzL1T9iZqt+pLjNbHMsLa0aKSszsRrLiLSAAg=",
            ]
        }
    )
    expected_result_df = df_personer.clone().update(expected_result_fnr_df)
    result = (
        Pseudonymize.from_polars(df_personer)
        .on_fields("fnr")
        .with_default_encryption()
        .run()
    )
    assert result.to_polars().equals(expected_result_df)


@integration_test()
def test_pseudonymize_sid(
    setup: Generator[None, None, None], df_personer: pl.DataFrame
) -> None:
    expected_result_fnr_df = pl.DataFrame(
        {
            "fnr": [
                "jJuuj0i",
                "ylc9488",
                "yeLfkaL",
            ]
        }
    )
    expected_result_df = df_personer.clone().update(expected_result_fnr_df)
    result = (
        Pseudonymize.from_polars(df_personer).on_fields("fnr").with_stable_id().run()
    )
    assert result.to_polars().equals(expected_result_df)
