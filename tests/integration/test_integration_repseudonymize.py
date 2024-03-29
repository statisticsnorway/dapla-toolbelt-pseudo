from collections.abc import Generator

import polars as pl

from dapla_pseudo import Repseudonymize
from tests.integration.utils import integration_test
from tests.integration.utils import setup


@integration_test()
def test_repseudonymize_from_default_encryption_to_fpe(
    setup: Generator[None, None, None],
    df_personer_fnr_ff31_encrypted: pl.DataFrame,
    df_personer_fnr_daead_encrypted: pl.DataFrame,
) -> None:
    result_df = (
        Repseudonymize.from_polars(df_personer_fnr_daead_encrypted)
        .on_fields("fnr")
        .from_default_encryption()
        .to_papis_compatible_encryption()
        .run()
        .to_polars()
    )
    assert result_df.equals(df_personer_fnr_ff31_encrypted)


@integration_test()
def test_repseudonymize_change_keys(
    setup: Generator[None, None, None],
    df_personer_daead_encrypted_ssb_common_key_2: pl.DataFrame,
    df_personer_fnr_daead_encrypted: pl.DataFrame,
) -> None:
    result = (
        Repseudonymize.from_polars(df_personer_fnr_daead_encrypted)
        .on_fields("fnr")
        .from_default_encryption()
        .to_default_encryption(custom_key="ssb-common-key-2")
        .run()
        .to_polars()
    )
    assert result.equals(df_personer_daead_encrypted_ssb_common_key_2)
