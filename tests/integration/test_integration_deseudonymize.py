from collections.abc import Generator

import polars as pl

from dapla_pseudo import Depseudonymize
from tests.integration.utils import df_personer
from tests.integration.utils import df_personer_fnr_daead_encrypted
from tests.integration.utils import integration_test
from tests.integration.utils import setup


@integration_test()
def test_depseudonymize_default_encryption(
    setup: Generator[None, None, None],
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
    assert result.equals(df_personer)
