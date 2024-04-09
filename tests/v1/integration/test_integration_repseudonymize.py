import polars as pl
import pytest
from polars.testing import assert_frame_equal

from dapla_pseudo import Repseudonymize
from tests.v1.integration.utils import integration_test


@pytest.mark.usefixtures("setup")
@integration_test()
def test_repseudonymize_from_default_encryption_to_fpe(
    df_personer_fnr_ff31_encrypted: pl.DataFrame,
    df_personer_fnr_daead_encrypted: pl.DataFrame,
) -> None:
    result = (
        Repseudonymize.from_polars(df_personer_fnr_daead_encrypted)
        .on_fields("fnr")
        .from_default_encryption()
        .to_papis_compatible_encryption()
        .run()
        .to_polars()
    )
    assert_frame_equal(result, df_personer_fnr_ff31_encrypted)


@pytest.mark.usefixtures("setup")
@integration_test()
def test_repseudonymize_change_keys(
    df_personer_daead_encrypted_ssb_common_key_2: pl.DataFrame,
    df_personer_daead_encrypted_ssb_common_key_1: pl.DataFrame,
) -> None:
    result = (
        Repseudonymize.from_polars(df_personer_daead_encrypted_ssb_common_key_1)
        .on_fields("fnr", "fornavn", "etternavn")
        .from_default_encryption()
        .to_default_encryption(custom_key="ssb-common-key-2")
        .run()
        .to_polars()
    )
    assert_frame_equal(result, df_personer_daead_encrypted_ssb_common_key_2)


@pytest.mark.usefixtures("setup")
@integration_test()
def test_repseudonymize_from_sid_to_daead(
    df_personer: pl.DataFrame,
    df_personer_daead_encrypted_ssb_common_key_1: pl.DataFrame,
    df_personer_daead_encrypted_ssb_common_key_2: pl.DataFrame,
    df_personer_pseudo_stable_id_daead_encrypted_ssb_common_key_2: pl.DataFrame,
) -> None:
    result = (
        Repseudonymize.from_polars(
            df_personer_pseudo_stable_id_daead_encrypted_ssb_common_key_2
        )
        .on_fields("fnr")
        .from_stable_id()
        .to_default_encryption()
        .on_fields("fornavn", "etternavn")
        .from_default_encryption(custom_key="ssb-common-key-2")
        .to_default_encryption(custom_key="ssb-common-key-1")
        .run()
        .to_polars()
    )
    assert_frame_equal(result, df_personer_daead_encrypted_ssb_common_key_1)
