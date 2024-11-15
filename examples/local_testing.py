## This file is a skeleton for testing/debugging parts of the client application
## Simply activate the virtual environment for this package,
## make adjustments in this file for your usecase, and run

import os
import subprocess
from random import choice
from string import ascii_lowercase
from string import digits

import polars as pl

from dapla_pseudo import Pseudonymize

chars = ascii_lowercase + digits

str_length = 40
n = 40
lst = ["".join(choice(chars) for _ in range(str_length)) for _ in range(n)]

df = pl.from_dict({"pseudo": lst, "pseudo2": lst, "pseudo3": lst})

os.environ["PSEUDO_SERVICE_URL"] = "https://dapla-pseudo-service.staging-bip-app.ssb.no"
subprocess.run(["gcloud", "config", "set", "core/disable_file_logging", "True"])
id_token = subprocess.getoutput("gcloud auth print-identity-token")
os.environ["PSEUDO_SERVICE_AUTH_TOKEN"] = id_token

result = (
    Pseudonymize.from_polars(df)
    .on_fields("pseudo")
    .with_default_encryption()
    .run(timeout=3000)
)
