import json
import typing as t

import pandas as pd
import polars as pl
import pytest
from dapla_metadata.datasets.core import Datadoc
from pandas.testing import assert_frame_equal as pd_assert_frame_equal
from polars.testing import assert_frame_equal as pl_assert_frame_equal
from tests.v1.integration.utils import get_expected_datadoc_metadata_variables
from tests.v1.integration.utils import integration_test

from dapla_pseudo import Depseudonymize
from dapla_pseudo import Pseudonymize
from dapla_pseudo import Repseudonymize
from dapla_pseudo.utils import encode_datadoc_variables


@pytest.mark.usefixtures("setup")
@integration_test()
def test_repseudonymize_with_metadata(
    df_personer_fnr_daead_encrypted_metadata: Datadoc,
    df_personer_fnr_daead_encrypted: pl.DataFrame,
) -> None:
    """This test ensures that when using the 'with_metadata' method when repseudonymizing you get the expected metadata."""
    result = (
        Repseudonymize.from_polars(df_personer_fnr_daead_encrypted)
        .with_metadata(df_personer_fnr_daead_encrypted_metadata)
        .on_fields("fnr")
        .from_default_encryption()
        .to_papis_compatible_encryption()
        .run()
    )
    assert isinstance(result.datadoc_model, dict)
    datadoc_variables: list[dict[str, t.Any]] = result.datadoc_model["datadoc"][
        "variables"
    ]
    if fnr_variable := next(
        filter(lambda v: v["short_name"] == "fnr", datadoc_variables), None
    ):
        expected = {
            "encryption_algorithm": "TINK-FPE",
            "encryption_key_reference": "papis-common-key-1",
            "encryption_algorithm_parameters": [
                {"keyId": "papis-common-key-1"},
                {"strategy": "skip"},
            ],
        }
        assert {
            k: v for k, v in fnr_variable["pseudonymization"].items() if v is not None
        } == expected
    else:
        pytest.fail("Pseudonymization object in 'fnr' variable metadata is missing.")


@pytest.mark.usefixtures("setup")
@integration_test()
def test_depseudonymize_with_metadata(
    df_personer_fnr_daead_encrypted_metadata: Datadoc,
    df_personer_fnr_daead_encrypted: pl.DataFrame,
) -> None:
    """This test ensures that when using the 'with_metadata' method when depseudonymizing you get the expected metadata."""
    result = (
        Depseudonymize.from_polars(df_personer_fnr_daead_encrypted)
        .with_metadata(df_personer_fnr_daead_encrypted_metadata)
        .on_fields("fnr")
        .with_default_encryption()
        .run()
    )
    # The index 0 here corresponds to the variable 'fnr'
    assert isinstance(result.datadoc_model, dict)
    datadoc_variables: list[dict[str, t.Any]] = result.datadoc_model["datadoc"][
        "variables"
    ]
    if fnr_variable := next(
        filter(lambda v: v["short_name"] == "fnr", datadoc_variables), None
    ):
        assert fnr_variable.get("pseudonymization") is None
    else:
        pytest.fail("Metadata for the 'fnr' variable is missing.")


@pytest.mark.usefixtures("setup")
@integration_test()
def test_pseudonymize_with_metadata(
    df_personer_metadata: Datadoc,
    df_personer: pl.DataFrame,
) -> None:
    """This test ensures that when using the 'with_metadata' method when pseudonymizing you get the expected metadata."""
    result = (
        Pseudonymize.from_polars(df_personer)
        .with_metadata(df_personer_metadata)
        .on_fields("fnr")
        .with_default_encryption()
        .run()
    )
    assert isinstance(result.datadoc_model, dict)
    datadoc_variables: list[dict[str, t.Any]] = result.datadoc_model["datadoc"][
        "variables"
    ]
    if fnr_variable := next(
        filter(lambda v: v["short_name"] == "fnr", datadoc_variables), None
    ):
        if fnr_variable_from_datadoc := next(
            filter(
                lambda v: v["short_name"] == "fnr",
                json.loads(result.datadoc)["datadoc"]["variables"],
            ),
            None,
        ):
            assert (
                fnr_variable["pseudonymization"]
                == fnr_variable_from_datadoc["pseudonymization"]
            )

        expected = {
            "encryption_algorithm": "TINK-DAEAD",
            "encryption_key_reference": "ssb-common-key-1",
            "encryption_algorithm_parameters": [{"keyId": "ssb-common-key-1"}],
        }
        assert {
            k: v for k, v in fnr_variable["pseudonymization"].items() if v is not None
        } == expected
    else:
        pytest.fail("Pseudonymization object in 'fnr' variable metadata is missing.")


@pytest.mark.usefixtures("setup")
@pytest.mark.parametrize(
    "output_func",
    [("pandas"), ("polars")],
)
@pytest.mark.parametrize(
    "input_func",
    [("pandas"), ("polars")],
)
@integration_test()
def test_pseudonymize_input_output_funcs(
    input_func: t.Literal["file", "pandas", "polars"],
    output_func: t.Literal["file", "pandas", "polars"],
    df_personer_pandas: pd.DataFrame,
    df_personer: pl.DataFrame,
    df_personer_fnr_daead_encrypted: pl.DataFrame,
    df_pandas_personer_fnr_daead_encrypted: pd.DataFrame,
) -> None:
    """This test runs several times, once for every combination of the possible input and output datatypes.

    It is intended to test for the conversion between data types, e.g. Polars DataFrame -> File.
    """
    match input_func:
        case "pandas":
            pseudonymizer = Pseudonymize.from_pandas(df_personer_pandas)
        case "polars":
            pseudonymizer = Pseudonymize.from_polars(df_personer)

    result = pseudonymizer.on_fields("fnr").with_default_encryption().run()

    match output_func:
        case "pandas":
            df_pandas = result.to_pandas()
            pd_assert_frame_equal(df_pandas, df_pandas_personer_fnr_daead_encrypted)
        case "polars":
            df_polars = result.to_polars()
            pl_assert_frame_equal(df_polars, df_personer_fnr_daead_encrypted)


@pytest.mark.usefixtures("setup")
@integration_test()
def test_pseudonymize_with_arrow_dtypes(
    pandas_diverse_datatypes: pd.DataFrame,
) -> None:
    """This test ensures that datatypes are retained when converting internally to and from Pandas."""
    result = (
        Pseudonymize.from_pandas(pandas_diverse_datatypes)
        .on_fields("fnr")
        .with_default_encryption()
        .run()
    )

    df_result = result.to_pandas()
    assert df_result.dtypes.equals(pandas_diverse_datatypes.dtypes)


@pytest.mark.usefixtures("setup")
@integration_test()
def test_pseudonymize_serialized_datadoc_variables(
    df_personer: pl.DataFrame,
) -> None:
    result = (
        Pseudonymize.from_polars(df_personer)
        .on_fields("fnr")
        .with_default_encryption()
        .run()
    )
    expected_metadata_container = get_expected_datadoc_metadata_variables(
        "test_pseudonymize_default_encryption"
    )

    assert result.datadoc == encode_datadoc_variables(expected_metadata_container)
    assert json.loads(result.datadoc) == json.loads(
        encode_datadoc_variables(expected_metadata_container)
    )


@pytest.mark.usefixtures("setup")
@integration_test()
def test_datadoc_variables_string_formatting(
    df_personer: pl.DataFrame,
) -> None:
    result = (
        Pseudonymize.from_polars(df_personer)
        .on_fields("fnr")
        .with_default_encryption()
        .run()
    )
    expected_variable_metadata = json.dumps(json.loads(result.datadoc), indent=2)
    # Ensure json string has correct indentation level
    assert expected_variable_metadata == result.datadoc


@pytest.mark.usefixtures("setup")
@integration_test()
def test_datadoc_model_string_formatting(
    df_personer_metadata: Datadoc,
    df_personer: pl.DataFrame,
) -> None:
    result = (
        Pseudonymize.from_polars(df_personer)
        .with_metadata(df_personer_metadata)
        .on_fields("fnr")
        .with_default_encryption()
        .run()
    )
    expected_datadoc_metadata = json.dumps(json.loads(result.datadoc), indent=2)
    # Ensure json string has correct indentation level
    assert expected_datadoc_metadata == result.datadoc
