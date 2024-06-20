"""Performance test the traversal part of dapla-toolbelt-pseudo.

This file times the running of the traversal.

You can also run Austin to get a more detailed overview over the time used.

Example, running from root:
```bash
poetry shell
python tests/perf/traverse_tester.py

## Profile and build flamegraph of time spent
## 'flamegraph.pl' binary is needed to be installed as a system package
sudo austin -P .venv/bin/python tests/perf/traverse_tester.py | flamegraph.pl > fg.svg
open fg.svg
```
"""

import json
import time

import polars as pl
from dapla import FileClient

from dapla_pseudo.v1.models.core import PseudoRule
from dapla_pseudo.v1.mutable_dataframe import MutableDataFrame

fs = FileClient.get_gcs_file_system()
DATA_PATH = "gs://ssb-dapla-felles-data-delt-test/test/skatt-person/skattemelding/2024/2024-03-05T11_22_16.226Z_4268_27358258_1.parquet"
RULES_PATH = "tests/perf/pseudo_rules_2024.json"

with open(RULES_PATH, mode="rb") as rules_file:
    content = json.loads(rules_file.read())
    rules = [PseudoRule.from_json(rule) for rule in content]

with fs.open(DATA_PATH, mode="rb") as f:
    df_in = pl.read_parquet(f)


def match_rules(df: MutableDataFrame, rules: list[PseudoRule]) -> None:
    df.match_rules(rules, None)


## Warm up cache
for _ in range(5):
    df = MutableDataFrame(df_in, hierarchical=True)
    match_rules(df, rules)

df = MutableDataFrame(df_in, hierarchical=True)
start = time.time()
match_rules(df, rules)
end = time.time()

print(f"Time elapsed: {end - start}")

print("Done!")
