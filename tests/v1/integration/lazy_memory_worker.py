import argparse
import json
import os
import platform
import resource
import threading
import time

import polars as pl

from dapla_pseudo import Pseudonymize


def _peak_rss_bytes() -> float:
    peak_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if platform.system() == "Darwin":
        return float(peak_rss)
    return float(peak_rss) * 1024.0


def _current_rss_bytes() -> float:
    """Return current RSS when available, otherwise best-effort fallback.

    Linux exposes current RSS via /proc/self/statm.
    macOS does not expose current RSS in the Python stdlib in the same way,
    so we fall back to peak RSS from getrusage.
    """
    system = platform.system()

    if system == "Linux":
        page_size = os.sysconf("SC_PAGE_SIZE")
        with open("/proc/self/statm", encoding="utf-8") as statm_file:
            rss_pages = int(statm_file.read().split()[1])
        return float(rss_pages * page_size)

    if system == "Darwin":
        return _peak_rss_bytes()

    return _peak_rss_bytes()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-path", required=True)
    parser.add_argument("--fields", nargs="+", required=True)
    args = parser.parse_args()

    baseline_rss_bytes = _current_rss_bytes()
    peak_current_rss_bytes = baseline_rss_bytes
    is_sampling = True

    def sample_rss() -> None:
        nonlocal peak_current_rss_bytes, is_sampling
        while is_sampling:
            current_rss_bytes = _current_rss_bytes()
            if current_rss_bytes > peak_current_rss_bytes:
                peak_current_rss_bytes = current_rss_bytes
            time.sleep(0.01)

    sampler_thread = threading.Thread(target=sample_rss, daemon=True)
    sampler_thread.start()

    lazy_df = pl.scan_parquet(args.input_path)
    result = (
        Pseudonymize.from_polars(lazy_df)
        .on_fields(*args.fields)
        .with_default_encryption()
        .run()
    )
    _ = result.metadata

    is_sampling = False
    sampler_thread.join(timeout=1)

    rss_increase_bytes = max(peak_current_rss_bytes - baseline_rss_bytes, 1.0)

    print(
        json.dumps(
            {
                "rss_increase_bytes": rss_increase_bytes,
                "peak_rss_bytes": _peak_rss_bytes(),
                "baseline_rss_bytes": baseline_rss_bytes,
            }
        )
    )


if __name__ == "__main__":
    main()
