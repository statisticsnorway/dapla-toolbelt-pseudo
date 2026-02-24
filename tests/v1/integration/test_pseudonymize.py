import asyncio

import polars as pl
import pytest
from polars.testing import assert_frame_equal
from tests.v1.integration.utils import get_calling_function_name
from tests.v1.integration.utils import get_expected_datadoc_metadata_variables
from tests.v1.integration.utils import integration_test

from dapla_pseudo import Pseudonymize
from dapla_pseudo.utils import encode_datadoc_variables
from dapla_pseudo.v1.result import Result


@pytest.mark.usefixtures("setup")
@integration_test()
def test_pseudonymize_default_encryption(
    df_personer: pl.DataFrame,
    df_personer_fnr_daead_encrypted: pl.DataFrame,
) -> None:
    result = (
        Pseudonymize.from_polars(df_personer)
        .on_fields("fnr")
        .with_default_encryption()
        .run()
    )
    current_function_name = get_calling_function_name()
    expected_metadata_container = get_expected_datadoc_metadata_variables(
        current_function_name
    )

    assert result.datadoc == encode_datadoc_variables(expected_metadata_container)
    assert_frame_equal(result.to_polars(), df_personer_fnr_daead_encrypted)


@pytest.mark.usefixtures("setup")
@integration_test()
def test_pseudonymize_papis_compatible_encryption(
    df_personer: pl.DataFrame,
    df_personer_fnr_ff31_encrypted: pl.DataFrame,
) -> None:
    result = (
        Pseudonymize.from_polars(df_personer)
        .on_fields("fnr")
        .with_papis_compatible_encryption()
        .run()
    )
    current_function_name = get_calling_function_name()
    expected_metadata_container = get_expected_datadoc_metadata_variables(
        current_function_name
    )

    assert result.datadoc == encode_datadoc_variables(expected_metadata_container)
    assert_frame_equal(result.to_polars(), df_personer_fnr_ff31_encrypted)


@pytest.mark.usefixtures("setup")
@integration_test()
def test_pseudonymize_default_encryption_null(
    df_personer: pl.DataFrame,
    df_personer_fnr_daead_encrypted: pl.DataFrame,
) -> None:
    fnr_values = [*df_personer["fnr"].to_list(), None]

    df_personer = df_personer.update(pl.DataFrame({"fnr": fnr_values}))
    result = (
        Pseudonymize.from_polars(df_personer)
        .on_fields("fnr")
        .with_default_encryption()
        .run()
    )
    current_function_name = get_calling_function_name()
    expected_metadata_container = get_expected_datadoc_metadata_variables(
        current_function_name
    )

    assert result.datadoc == encode_datadoc_variables(expected_metadata_container)
    assert_frame_equal(result.to_polars(), df_personer_fnr_daead_encrypted)


@pytest.mark.usefixtures("setup")
@integration_test()
def test_pseudonymize_sid(
    df_personer: pl.DataFrame,
    df_personer_sid_fnr: pl.DataFrame,
) -> None:
    result = (
        Pseudonymize.from_polars(df_personer)
        .on_fields("fnr")
        .with_stable_id(sid_snapshot_date="2026-01-21")
        .run()
    )

    current_function_name = get_calling_function_name()
    expected_metadata_container = get_expected_datadoc_metadata_variables(
        current_function_name
    )
    assert result.datadoc == encode_datadoc_variables(expected_metadata_container)
    assert_frame_equal(result.to_polars(), df_personer_sid_fnr)


@pytest.mark.usefixtures("setup")
@integration_test()
def test_pseudonymize_sid_null(df_personer: pl.DataFrame) -> None:
    expected_result_fnr_df = pl.DataFrame(
        {"fnr": ["jJuuj0i", "ylc9488", "yeLfkaL", None]}
    )

    df_personer = pl.DataFrame({"fnr": [*df_personer["fnr"].to_list(), None]})

    result = (
        Pseudonymize.from_polars(df_personer).on_fields("fnr").with_stable_id().run()
    )
    current_function_name = get_calling_function_name()
    expected_metadata_container = get_expected_datadoc_metadata_variables(
        current_function_name
    )

    assert result.datadoc == encode_datadoc_variables(expected_metadata_container)
    assert result.metadata["metrics"]["MAPPED_SID"] == 3
    assert result.metadata["metrics"]["NULL_VALUE"] == 1
    assert_frame_equal(result.to_polars(), expected_result_fnr_df)


@pytest.mark.usefixtures("setup")
@integration_test()
def test_pseudonymize_default_encryption_synchronous(
    df_personer: pl.DataFrame, df_personer_fnr_daead_encrypted: pl.DataFrame
) -> None:
    """Initialize an event loop to simulate running in an environment with a running event loop (e.g. Jupyter Notebook)."""

    async def async_wrapper() -> Result:
        """Simply presents asyncio with the correct interface in order to run the function in an event loop."""
        return (
            Pseudonymize.from_polars(df_personer)
            .on_fields("fnr")
            .with_default_encryption()
            .run()
        )

    result = asyncio.run(async_wrapper())
    current_function_name = get_calling_function_name()
    expected_metadata_container = get_expected_datadoc_metadata_variables(
        current_function_name
    )

    assert result.datadoc == encode_datadoc_variables(expected_metadata_container)
    assert_frame_equal(result.to_polars(), df_personer_fnr_daead_encrypted)


@pytest.mark.usefixtures("setup")
@integration_test()
def test_pseudonymize_default_encryption_lazyframe(
    df_personer: pl.DataFrame,
    df_personer_fnr_daead_encrypted: pl.DataFrame,
) -> None:
    eager_result = (
        Pseudonymize.from_polars(df_personer)
        .on_fields("fnr")
        .with_default_encryption()
        .run()
        .to_polars()
    )

    result = (
        Pseudonymize.from_polars_lazy(df_personer.lazy())
        .on_fields("fnr")
        .with_default_encryption()
        .run()
        .to_polars()
    )

    assert result.shape == eager_result.shape
    assert result.schema == eager_result.schema
    assert_frame_equal(result, df_personer_fnr_daead_encrypted)


@pytest.mark.usefixtures("setup")
@integration_test()
def test_pseudonymize_hierarchical_not_supported_for_lazyframe(
    df_personer: pl.DataFrame,
) -> None:
    with pytest.raises(
        ValueError,
        match="Hierarchical datasets are not supported for Polars LazyFrames.",
    ):
        (
            Pseudonymize.from_polars_lazy(df_personer.lazy())
            .on_fields("fnr")
            .with_default_encryption()
            .run(hierarchical=True)
        )
