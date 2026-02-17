import polars as pl
import pytest
from polars.testing import assert_frame_equal
from tests.v1.integration.utils import get_calling_function_name
from tests.v1.integration.utils import get_expected_datadoc_metadata_variables
from tests.v1.integration.utils import integration_test

from dapla_pseudo import Pseudonymize
from dapla_pseudo.constants import PseudoFunctionTypes
from dapla_pseudo.utils import encode_datadoc_variables
from dapla_pseudo.v1.models.core import DaeadKeywordArgs
from dapla_pseudo.v1.models.core import PseudoFunction
from dapla_pseudo.v1.models.core import PseudoRule
from dapla_pseudo.v1.models.core import RedactKeywordArgs


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
    expected_metadata_container = get_expected_datadoc_metadata_variables(
        current_function_name
    )

    assert result.datadoc == encode_datadoc_variables(expected_metadata_container)
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
    expected_metadata_container = get_expected_datadoc_metadata_variables(
        current_function_name
    )

    assert result.datadoc == encode_datadoc_variables(expected_metadata_container)
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
    expected_metadata_container = get_expected_datadoc_metadata_variables(
        current_function_name
    )

    assert result.datadoc == encode_datadoc_variables(expected_metadata_container)
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
    expected_metadata_container = get_expected_datadoc_metadata_variables(
        current_function_name
    )

    assert result.datadoc == encode_datadoc_variables(expected_metadata_container)
    assert_frame_equal(
        result.to_polars(), df_personer_hierarchical_inner_list_pseudonymized
    )


@pytest.mark.usefixtures("setup")
@integration_test()
def test_pseudonymize_hierarchical_complex(
    df_personer_hierarchical_complex: pl.DataFrame,
    df_personer_hierarchical_complex_pseudonymized: pl.DataFrame,
) -> None:
    rule = PseudoRule(
        name="my-rule",
        func=PseudoFunction(
            function_type=PseudoFunctionTypes.DAEAD, kwargs=DaeadKeywordArgs()
        ),
        pattern="**/person_info/barn/fnr",
        path="person_info/barn/fnr",
    )
    result = (
        Pseudonymize.from_polars(df_personer_hierarchical_complex)
        .add_rules(rule)
        .run(hierarchical=True)
    )

    assert_frame_equal(
        result.to_polars(), df_personer_hierarchical_complex_pseudonymized
    )


@pytest.mark.usefixtures("setup")
@integration_test()
def test_hierarchical_request_batching_daead(
    df_personer_hierarchical: pl.DataFrame,
    df_personer_hierarchical_pseudonymized: pl.DataFrame,
) -> None:
    """Verify hierarchical non-REDACT behavior is grouped into one logical field result.

    This test asserts real end-to-end behavior without patching internals. For the
    chosen fixture there are three leaf paths under ``person_info/fnr``. If requests
    are batched, the result metadata is aggregated under a single field key.
    """
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

    datadoc_model = result.datadoc_model
    assert isinstance(datadoc_model, list)
    assert len(datadoc_model) == 1
    assert datadoc_model[0]["data_element_path"] == "person_info.fnr"
    assert_frame_equal(result.to_polars(), df_personer_hierarchical_pseudonymized)


@pytest.mark.usefixtures("setup")
@integration_test()
def test_hierarchical_request_batching_redact_not_grouped(
    df_personer_hierarchical: pl.DataFrame,
    df_personer_hierarchical_redacted: pl.DataFrame,
) -> None:
    """Verify hierarchical REDACT keeps per-leaf field granularity.

    This also uses real behavior only. REDACT is intentionally not grouped, so the
    metadata details should keep one key per concrete matched leaf path.
    """
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

    datadoc_model = result.datadoc_model
    assert isinstance(datadoc_model, list)
    assert len(datadoc_model) == 3
    assert {entry["data_element_path"] for entry in datadoc_model} == {
        "person_info[0].fnr",
        "person_info[1].fnr",
        "person_info[2].fnr",
    }
    assert_frame_equal(result.to_polars(), df_personer_hierarchical_redacted)
