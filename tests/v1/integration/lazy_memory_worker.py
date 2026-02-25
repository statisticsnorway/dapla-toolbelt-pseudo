import argparse
import json
import platform
import resource

import polars as pl

from dapla_pseudo import Pseudonymize


def _peak_rss_bytes() -> float:
    peak_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if platform.system() == "Darwin":
        return float(peak_rss)
    return float(peak_rss) * 1024.0


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-path", required=True)
    parser.add_argument("--fields", nargs="+", required=True)
    args = parser.parse_args()

    lazy_df = pl.scan_parquet(args.input_path)
    result = (
        Pseudonymize.from_polars(lazy_df)
        .on_fields(*args.fields)
        .with_default_encryption()
        .run()
    )
    _ = result.to_polars()

    print(json.dumps({"peak_rss_bytes": _peak_rss_bytes()}))


if __name__ == "__main__":
    main()
