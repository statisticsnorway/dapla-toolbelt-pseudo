import os

import polars as pl
import pytest
from polars.testing import assert_frame_equal
from tests.v1.integration.utils import get_expected_datadoc_metadata_container
from tests.v1.integration.utils import integration_test

from dapla_pseudo import Pseudonymize


@pytest.mark.usefixtures("setup")
@integration_test()
def test_pseudonymize_default_encryption_multiple_partitions(
    df_personer: pl.DataFrame,
    df_personer_fnr_daead_encrypted: pl.DataFrame,
) -> None:
    os.environ["PSEUDO_CLIENT_ROWS_PER_PARTITION"] = "1"
    result = (
        Pseudonymize.from_polars(df_personer)
        .on_fields("fnr")
        .with_default_encryption()
        .run()
    )
    expected_metadata_container = get_expected_datadoc_metadata_container(
        "test_pseudonymize_default_encryption"
    )

    assert result.datadoc == expected_metadata_container.model_dump_json(
        exclude_none=True
    )
    assert_frame_equal(result.to_polars(), df_personer_fnr_daead_encrypted)
