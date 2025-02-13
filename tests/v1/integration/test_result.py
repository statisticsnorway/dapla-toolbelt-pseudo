import json
import typing as t
from pathlib import Path

import pandas as pd
import polars as pl
import pytest
from pandas.testing import assert_frame_equal as pd_assert_frame_equal
from polars.testing import assert_frame_equal as pl_assert_frame_equal
from tests.v1.integration.utils import integration_test

from dapla_pseudo import Pseudonymize
from dapla_pseudo.v1.depseudo import Depseudonymize


@pytest.mark.usefixtures("setup")
@pytest.mark.parametrize(
    "output_func",
    [("file"), ("pandas"), ("polars")],
)
@pytest.mark.parametrize(
    "input_func",
    [("file"), ("pandas"), ("polars")],
)
@integration_test()
def test_pseudonymize_input_output_funcs(
    input_func: t.Literal["file", "pandas", "polars"],
    output_func: t.Literal["file", "pandas", "polars"],
    tmp_path: Path,
    personer_file_path: str,
    df_personer_pandas: pd.DataFrame,
    df_personer: pl.DataFrame,
    df_personer_fnr_daead_encrypted: pl.DataFrame,
    df_pandas_personer_fnr_daead_encrypted: pd.DataFrame,
) -> None:
    """This test runs several times, once for every combination of the possible input and output datatypes.

    It is intended to test for the conversion between data types, e.g. Polars DataFrame -> File.
    """
    match input_func:
        case "file":
            pseudonymizer = Pseudonymize.from_file(personer_file_path)
        case "pandas":
            pseudonymizer = Pseudonymize.from_pandas(df_personer_pandas)
        case "polars":
            pseudonymizer = Pseudonymize.from_polars(df_personer)

    result = pseudonymizer.on_fields("fnr").with_default_encryption().run()

    match output_func:
        case "file":
            file_path = tmp_path / "personer_pseudo.json"
            result.to_file(str(file_path))

            expected = json.loads(
                open("tests/data/personer_pseudonymized_default_encryption.json").read()
            )
            actual = json.loads(file_path.open().read())

            assert expected == actual
        case "pandas":
            df_pandas = result.to_pandas()
            pd_assert_frame_equal(df_pandas, df_pandas_personer_fnr_daead_encrypted)
        case "polars":
            df_polars = result.to_polars()
            pl_assert_frame_equal(df_polars, df_personer_fnr_daead_encrypted)


@pytest.mark.usefixtures("setup")
@integration_test()
def test_pseudonymize_add_results(
    df_personer: pl.DataFrame,
    df_personer_fnr_daead_encrypted: pl.DataFrame,
) -> None:
    # create a new column with a pseudonymized FNR
    df_personer = df_personer.with_columns(
        df_personer_fnr_daead_encrypted.get_column("fnr").alias("fnr_pseudo")
    )

    result = (
        Pseudonymize.from_polars(df_personer)
        .on_fields("fnr")
        .with_default_encryption()
        .run()
    )

    assert result.to_polars().drop("fnr_pseudo").equals(df_personer_fnr_daead_encrypted)

    result_new = (
        Depseudonymize.from_result(result)
        .on_fields("fnr_pseudo")
        .with_default_encryption()
        .run()
    )

    assert (
        result_new._datadoc == result._datadoc
    )  # Depseudonymization doesn't return Datadoc metadata
    assert result_new._metadata == {
        **result._metadata,
        "fnr_pseudo": result_new._metadata["fnr_pseudo"],
    }
