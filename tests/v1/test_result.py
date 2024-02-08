import io
from pathlib import Path

import pandas as pd
import polars as pl

from dapla_pseudo.v1.result import Result


def test_result_index_level(tmp_path: Path) -> None:
    # Related to https://github.com/pola-rs/polars/issues/7291
    # If this test fails, hopefully this means that the issue is fixed
    # and the test can be removed, as well as the code in Result
    # removing the column "__index_level_0__"

    df = pd.read_csv(
        io.StringIO(
            """
            a	b
            1	4
            2	5
            3	6
        """
        ),
        sep="\t",
    )

    path_filtered = f"{tmp_path}/filtered.parquet"
    df.query("b % 2 == 0").to_parquet(path_filtered, engine="pyarrow")
    df_pl_filtered = pl.read_parquet(path_filtered)
    assert "__index_level_0__" in df_pl_filtered.columns

    df_result = Result(df_pl_filtered).to_polars()
    assert "__index_level_0__" not in df_result.columns
