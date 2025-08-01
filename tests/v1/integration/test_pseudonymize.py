import asyncio

import polars as pl
import pytest
from polars.testing import assert_frame_equal
from tests.v1.integration.utils import get_calling_function_name
from tests.v1.integration.utils import get_expected_datadoc_metadata_container
from tests.v1.integration.utils import integration_test

from dapla_pseudo import Pseudonymize
from dapla_pseudo.constants import PseudoFunctionTypes
from dapla_pseudo.v1.models.core import DaeadKeywordArgs
from dapla_pseudo.v1.models.core import PseudoFunction
from dapla_pseudo.v1.models.core import PseudoRule
from dapla_pseudo.v1.models.core import RedactKeywordArgs
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
    expected_metadata_container = get_expected_datadoc_metadata_container(
        current_function_name
    )

    assert result.datadoc == expected_metadata_container.model_dump_json(
        exclude_none=True
    )
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
    expected_metadata_container = get_expected_datadoc_metadata_container(
        current_function_name
    )

    assert result.datadoc == expected_metadata_container.model_dump_json(
        exclude_none=True
    )
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
    expected_metadata_container = get_expected_datadoc_metadata_container(
        current_function_name
    )

    assert result.datadoc == expected_metadata_container.model_dump_json(
        exclude_none=True
    )
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
        .with_stable_id(sid_snapshot_date="2023-08-31")
        .run()
    )

    current_function_name = get_calling_function_name()
    expected_metadata_container = get_expected_datadoc_metadata_container(
        current_function_name
    )
    assert result.datadoc == expected_metadata_container.model_dump_json(
        exclude_none=True
    )
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
    expected_metadata_container = get_expected_datadoc_metadata_container(
        current_function_name
    )

    assert result.datadoc == expected_metadata_container.model_dump_json(
        exclude_none=True
    )
    assert result.metadata["metrics"]["MAPPED_SID"] == 3
    assert result.metadata["metrics"]["NULL_VALUE"] == 1
    assert_frame_equal(result.to_polars(), expected_result_fnr_df)


@pytest.mark.usefixtures("setup")
@integration_test()
def test_pseudonymize_hierarchical(
    df_personer_hierarchical: pl.DataFrame,
    df_personer_hierarchical_pseudonymized: pl.DataFrame,
) -> None:
    rule = PseudoRule(
        name="my-rule",
        func=PseudoFunction(
            function_type=PseudoFunctionTypes.DAEAD, kwargs=DaeadKeywordArgs()
        ),
        pattern="**/person_info/fnr",
        path="person_info/fnr",
    )
    result = (
        Pseudonymize.from_polars(df_personer_hierarchical)
        .add_rules(rule)
        .run(hierarchical=True)
    )

    current_function_name = get_calling_function_name()
    expected_metadata_container = get_expected_datadoc_metadata_container(
        current_function_name
    )

    assert result.datadoc == expected_metadata_container.model_dump_json(
        exclude_none=True
    )
    assert_frame_equal(result.to_polars(), df_personer_hierarchical_pseudonymized)


@pytest.mark.usefixtures("setup")
@integration_test()
def test_pseudonymize_hierarchical_null(
    df_personer_hierarchical_null: pl.DataFrame,
    df_personer_hierarchical_null_pseudonymized: pl.DataFrame,
) -> None:
    rule = PseudoRule(
        name="my-rule",
        func=PseudoFunction(
            function_type=PseudoFunctionTypes.DAEAD, kwargs=DaeadKeywordArgs()
        ),
        pattern="**/person_info/fnr",
        path="person_info/fnr",
    )
    result = (
        Pseudonymize.from_polars(df_personer_hierarchical_null)
        .add_rules(rule)
        .run(hierarchical=True)
    )

    current_function_name = get_calling_function_name()
    expected_metadata_container = get_expected_datadoc_metadata_container(
        current_function_name
    )

    assert result.datadoc == expected_metadata_container.model_dump_json(
        exclude_none=True
    )
    assert_frame_equal(result.to_polars(), df_personer_hierarchical_null_pseudonymized)


@pytest.mark.usefixtures("setup")
@integration_test()
def test_pseudonymize_hierarchical_redact(
    df_personer_hierarchical: pl.DataFrame,
    df_personer_hierarchical_redacted: pl.DataFrame,
) -> None:
    rule = PseudoRule(
        name="my-rule",
        func=PseudoFunction(
            function_type=PseudoFunctionTypes.REDACT,
            kwargs=RedactKeywordArgs(placeholder=":"),
        ),
        pattern="**/person_info/fnr",
        path="person_info/fnr",
    )
    result = (
        Pseudonymize.from_polars(df_personer_hierarchical)
        .add_rules(rule)
        .run(hierarchical=True)
    )

    current_function_name = get_calling_function_name()
    expected_metadata_container = get_expected_datadoc_metadata_container(
        current_function_name
    )

    assert result.datadoc == expected_metadata_container.model_dump_json(
        exclude_none=True
    )
    assert_frame_equal(result.to_polars(), df_personer_hierarchical_redacted)


@pytest.mark.usefixtures("setup")
@integration_test()
def test_pseudonymize_hierarchical_inner_list(
    df_personer_hierarchical_inner_list: pl.DataFrame,
    df_personer_hierarchical_inner_list_pseudonymized: pl.DataFrame,
) -> None:
    rule = PseudoRule(
        name="my-rule",
        func=PseudoFunction(
            function_type=PseudoFunctionTypes.DAEAD, kwargs=DaeadKeywordArgs()
        ),
        pattern="**/values",
        path="identifiers/values",
    )
    result = (
        Pseudonymize.from_polars(df_personer_hierarchical_inner_list)
        .add_rules(rule)
        .run(hierarchical=True)
    )

    current_function_name = get_calling_function_name()
    expected_metadata_container = get_expected_datadoc_metadata_container(
        current_function_name
    )

    assert result.datadoc == expected_metadata_container.model_dump_json(
        exclude_none=True
    )
    assert_frame_equal(
        result.to_polars(), df_personer_hierarchical_inner_list_pseudonymized
    )


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
    expected_metadata_container = get_expected_datadoc_metadata_container(
        current_function_name
    )

    assert result.datadoc == expected_metadata_container.model_dump_json(
        exclude_none=True
    )
    assert_frame_equal(result.to_polars(), df_personer_fnr_daead_encrypted)
