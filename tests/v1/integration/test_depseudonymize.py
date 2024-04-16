import polars as pl
import pytest
from polars.testing import assert_frame_equal
from tests.v1.integration.utils import integration_test

from dapla_pseudo import Depseudonymize


@pytest.mark.usefixtures("setup")
@integration_test()
def test_depseudonymize_default_encryption(
    df_personer: pl.DataFrame,
    df_personer_fnr_daead_encrypted: pl.DataFrame,
) -> None:
    result = (
        Depseudonymize.from_polars(df_personer_fnr_daead_encrypted)
        .on_fields("fnr")
        .with_default_encryption()
        .run()
        .to_polars()
    )

    assert_frame_equal(result, df_personer)


@pytest.mark.usefixtures("setup")
@integration_test()
def test_depseudonymize_sid(
    df_personer_pseudo_stable_id_daead_encrypted_ssb_common_key_2: pl.DataFrame,
    df_personer: pl.DataFrame,
) -> None:
    result = (
        Depseudonymize.from_polars(
            df_personer_pseudo_stable_id_daead_encrypted_ssb_common_key_2
        )
        .on_fields("fnr")
        .with_stable_id()
        .on_fields("fornavn", "etternavn")
        .with_default_encryption(custom_key="ssb-common-key-2")
        .run()
        .to_polars()
    )
    assert_frame_equal(result, df_personer)
