from collections.abc import Generator

import polars as pl

from dapla_pseudo import Pseudonymize
from tests.integration.utils import get_calling_function_name
from tests.integration.utils import get_expected_datadoc_metadata_container
from tests.integration.utils import integration_test
from tests.integration.utils import setup


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
    current_function_name = get_calling_function_name()
    expected_metadata_container = get_expected_datadoc_metadata_container(
        current_function_name
    )

    assert result.datadoc == expected_metadata_container.model_dump_json(exclude_none=True)
    assert result.to_polars().equals(expected_result_df)


@integration_test()
def test_pseudonymize_papis_compatible_encryption(
    setup: Generator[None, None, None], df_personer: pl.DataFrame
) -> None:
    expected_result_fnr_df = pl.DataFrame(
        {
            "fnr": [
                "KsJu12NHdoZ",
                "QqaTeUtXvjk",
                "rAHb6rOyFtA",
            ]
        }
    )
    expected_result_df = df_personer.clone().update(expected_result_fnr_df)
    result = (
        Pseudonymize.from_polars(df_personer)
        .on_fields("fnr")
        .with_papis_compatible_encryption()
        .run()
    )
    current_function_name = get_calling_function_name()
    expected_metadata_container = get_expected_datadoc_metadata_container(
        current_function_name
    )

    assert result.datadoc == expected_metadata_container.model_dump_json(exclude_none=True)
    assert result.to_polars().equals(expected_result_df)


@integration_test()
def test_pseudonymize_default_encryption_null(
    setup: Generator[None, None, None], df_personer: pl.DataFrame
) -> None:
    expected_result_fnr_df = pl.DataFrame(
        {
            "fnr": [
                "AWIRfKLSNfR0ID+wBzogEcUT7JQPayk7Gosij6SXr8s=",
                "AWIRfKKLagk0LqYCKpiC4xfPkHqIWGVfc3wg5gUwRNE=",
                "AWIRfKIzL1T9iZqt+pLjNbHMsLa0aKSszsRrLiLSAAg=",
                None,
            ]
        }
    )
    expected_result_df = df_personer.clone().update(expected_result_fnr_df)

    fnr_values = [*df_personer["fnr"].to_list(), None]

    df_personer = df_personer.update(pl.DataFrame({"fnr": fnr_values}))
    result = (
        Pseudonymize.from_polars(df_personer)
        .on_fields("fnr")
        .with_default_encryption()
        .run()
    )
    current_function_name = get_calling_function_name()
    expected_metadata_container = get_expected_datadoc_metadata_container(
        current_function_name
    )

    assert result._datadoc == expected_metadata_container
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
    current_function_name = get_calling_function_name()
    expected_metadata_container = get_expected_datadoc_metadata_container(
        current_function_name
    )
    assert result._datadoc == expected_metadata_container
    assert result.to_polars().equals(expected_result_df)


@integration_test()
def test_pseudonymize_sid_null(
    setup: Generator[None, None, None], df_personer: pl.DataFrame
) -> None:
    expected_result_fnr_df = pl.DataFrame(
        {"fnr": ["jJuuj0i", "ylc9488", "yeLfkaL", None]}
    )
    expected_result_df = df_personer.clone().update(expected_result_fnr_df)

    fnr_values = [*df_personer["fnr"].to_list(), None]

    df_personer = df_personer.update(pl.DataFrame({"fnr": fnr_values}))

    result = (
        Pseudonymize.from_polars(df_personer).on_fields("fnr").with_stable_id().run()
    )
    current_function_name = get_calling_function_name()
    expected_metadata_container = get_expected_datadoc_metadata_container(
        current_function_name
    )

    assert result._datadoc == expected_metadata_container
    assert result.to_polars().equals(expected_result_df)
