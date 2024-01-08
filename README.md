# Dapla Toolbelt Pseudo

[![PyPI](https://img.shields.io/pypi/v/dapla-toolbelt-pseudo.svg)][pypi status]
[![Status](https://img.shields.io/pypi/status/dapla-toolbelt-pseudo.svg)][pypi status]
[![Python Version](https://img.shields.io/pypi/pyversions/dapla-toolbelt-pseudo)][pypi status]
[![License](https://img.shields.io/pypi/l/dapla-toolbelt-pseudo)][license]

[![Documentation](https://github.com/statisticsnorway/dapla-toolbelt-pseudo/actions/workflows/docs.yml/badge.svg)][documentation]
[![Tests](https://github.com/statisticsnorway/dapla-toolbelt-pseudo/actions/workflows/tests.yml/badge.svg)][tests]
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=statisticsnorway_dapla-toolbelt-pseudo&metric=coverage)][sonarcov]
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=statisticsnorway_dapla-toolbelt-pseudo&metric=alert_status)][sonarquality]

[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)][pre-commit]
[![Black](https://img.shields.io/badge/code%20style-black-000000.svg)][black]
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![Poetry](https://img.shields.io/endpoint?url=https://python-poetry.org/badge/v0.json)][poetry]

[pypi status]: https://pypi.org/project/dapla-toolbelt-pseudo/
[documentation]: https://statisticsnorway.github.io/dapla-toolbelt-pseudo
[tests]: https://github.com/statisticsnorway/dapla-toolbelt-pseudo/actions?workflow=Tests

[sonarcov]: https://sonarcloud.io/summary/overall?id=statisticsnorway_dapla-toolbelt-pseudo
[sonarquality]: https://sonarcloud.io/summary/overall?id=statisticsnorway_dapla-toolbelt-pseudo
[pre-commit]: https://github.com/pre-commit/pre-commit
[black]: https://github.com/psf/black
[poetry]: https://python-poetry.org/

Pseudonymize, repseudonymize and depseudonymize data on Dapla.

## Features

### Pseudonymize

```python
from dapla_pseudo import PseudoData
import pandas as pd

file_path="data/personer.json"

df = pd.read_json(file_path) # Create DataFrame from file

# Example: Single field default encryption (DAEAD)
result_df = (
    PseudoData.from_pandas(df)                     # Specify what dataframe to use
    .on_field("fornavn")                           # Select the field to pseudonymize
    .pseudonymize()                                # Apply pseudonymization to the selected field
    .to_polars()                                   # Get the result as a polars dataframe
)

# Example: Multiple fields default encryption (DAEAD)
result_df = (
    PseudoData.from_pandas(df)                     # Specify what dataframe to use
    .on_fields("fornavn", "etternavn")             # Select multiple fields to pseudonymize
    .pseudonymize()                                # Apply pseudonymization to the selected fields
    .to_polars()                                   # Get the result as a polars dataframe
)

# Example: Single field sid mapping and pseudonymization (FPE)
result_df = (
    PseudoData.from_pandas(df)                     # Specify what dataframe to use
    .on_field("fnr")                               # Select the field to pseudonymize
    .map_to_stable_id()                            # Map the selected field to stable id
    .pseudonymize()                                # Apply pseudonymization to the selected fields
    .to_polars()                                   # Get the result as a polars dataframe
)
```

The default encryption algorithm is DAEAD (Deterministic Authenticated Encryption with Associated Data). However, if the
field is a valid Norwegian personal identification number (fnr, dnr), the recommended way to pseudonymize is to use
the function `map_to_stable_id()` to convert the identification number to a stable ID (SID) prior to pseudonymization.
In that case, the pseudonymization algorithm is FPE (Format Preserving Encryption).

### Validate SID mapping

```python
from dapla_pseudo import Validator
import pandas as pd

file_path="data/personer.json"

df = pd.read_json(file_path)

result = (
    Validator.from_pandas(df)                   # Specify what dataframe to use
    .on_field("fnr")                            # Select the field to validate
    .validate_map_to_stable_id()                # Validate that all the field values can be mapped to a SID
)
# The resulting dataframe contains the field values that didn't have a corresponding SID
result.to_pandas()
```

A `sid_snapshot_date` can also be specified to validate that the field values can be mapped to a SID at a specific date:

```python
from dapla_pseudo import Validator
from dapla_pseudo.utils import convert_to_date
import pandas as pd

file_path="data/personer.json"

df = pd.read_json(file_path)

result = (
    Validator.from_pandas(df)
    .on_field("fnr")
    .validate_map_to_stable_id(
        sid_snapshot_date=convert_to_date("2023-08-29")
    )
)
# Show metadata about the validation (e.g. which version of the SID catalog was used)
result.metadata
# Show the field values that didn't have a corresponding SID
result.to_pandas()
```

## Advanced usage

### Pseudonymize

#### Read from file systems

```python
from dapla_pseudo import PseudoData
from dapla import AuthClient


file_path="data/personer.json"

options = {
    # Specify data types of columns in the dataset
    "dtype" : { "fnr": "string","fornavn": "string","etternavn": "string","kjonn": "category","fodselsdato": "string"}
}

# Example: Read dataframe from file
result_df = (
    PseudoData.from_file(file_path, **options)     # Read the DataFrame from file
    .on_fields("fornavn", "etternavn")             # Select multiple fields to pseudonymize
    .pseudonymize()                                # Apply pseudonymization to the selected fields
    .to_polars()                                   # Get the result as a polars dataframe
)

# Example: Read dataframe from GCS bucket
options = {
    # Specify data types of columns in the dataset
    "dtype" : { "fnr": "string","fornavn": "string","etternavn": "string","kjonn": "category","fodselsdato": "string"},
    # Specify storage options for Google Cloud Storage (GCS)
    "storage_options" : {"token": AuthClient.fetch_google_credentials()}
}

gcs_file_path = "gs://ssb-staging-dapla-felles-data-delt/felles/pseudo-examples/andeby_personer.csv"

result_df = (
    PseudoData.from_file(gcs_file_path, **options) # Read DataFrame from GCS
    .on_fields("fornavn", "etternavn")             # Select multiple fields to pseudonymize
    .pseudonymize()                                # Apply pseudonymization to the selected fields
    .to_polars()                                   # Get the result as a polars dataframe
)
```

#### Pseudoyminize using a custom keyset

```python
from dapla_pseudo import pseudonymize

# Pseudonymize fields in a local file using the default key:
pseudonymize(file_path="./data/personer.json", fields=["fnr", "fornavn"])

# Pseudonymize fields in a local file, explicitly denoting the key to use:
pseudonymize(file_path="./data/personer.json", fields=["fnr", "fornavn"], key="ssb-common-key-1")

# Pseudonymize a local file using a custom key:
import json
custom_keyset = json.dumps({
    "encryptedKeyset": "CiQAp91NBhLdknX3j9jF6vwhdyURaqcT9/M/iczV7fLn...8XYFKwxiwMtCzDT6QGzCCCM=",
    "keysetInfo": {
        "primaryKeyId": 1234567890,
        "keyInfo": [
            {
                "typeUrl": "type.googleapis.com/google.crypto.tink.AesSivKey",
                "status": "ENABLED",
                "keyId": 1234567890,
                "outputPrefixType": "TINK",
            }
        ],
    },
    "kekUri": "gcp-kms://projects/some-project-id/locations/europe-north1/keyRings/some-keyring/cryptoKeys/some-kek-1",
})
pseudonymize(file_path="./data/personer.json", fields=["fnr", "fornavn"], key=custom_keyset)

# Operate on data in a streaming manner:
import shutil
with pseudonymize("./data/personer.json", fields=["fnr", "fornavn", "etternavn"], stream=True) as res:
    with open("./data/personer_deid.json", 'wb') as f:
        res.raw.decode_content = True
        shutil.copyfileobj(res.raw, f)

# Map certain fields to stabil ID
pseudonymize(file_path="./data/personer.json", fields=["fornavn"], sid_fields=["fnr"])
```

### Repseudonymize

```python
from dapla_pseudo import repseudonymize

# Repseudonymize fields in a local file, denoting source and target keys to use:
repseudonymize(file_path="./data/personer_deid.json", fields=["fnr", "fornavn"], source_key="ssb-common-key-1", target_key="ssb-common-key-2")
```

### Depseudonymize

```python
from dapla_pseudo import depseudonymize

# Depseudonymize fields in a local file using the default key:
depseudonymize(file_path="./data/personer_deid.json", fields=["fnr", "fornavn"])

# Depseudonymize fields in a local file, explicitly denoting the key to use:
depseudonymize(file_path="./data/personer_deid.json", fields=["fnr", "fornavn"], key="ssb-common-key-1")
```

_Note that depseudonymization requires elevated access privileges._


## Requirements

- Python >= 3.10
- Dependencies can be found in `pyproject.toml`

## Installation

You can install _Dapla Toolbelt Pseudo_ via [pip] from [PyPI]:

```console
pip install dapla-toolbelt-pseudo
```

## Usage

Please see the [Reference Guide] for details.

## Contributing

Contributions are very welcome.
To learn more, see the [Contributor Guide].

## License

Distributed under the terms of the [MIT license][license],
_Dapla Toolbelt Pseudo_ is free and open source software.

## Issues

If you encounter any problems,
please [file an issue] along with a detailed description.

## Credits

This project was generated from [Statistics Norway]'s [SSB PyPI Template].

[statistics norway]: https://www.ssb.no/en
[pypi]: https://pypi.org/
[ssb pypi template]: https://github.com/statisticsnorway/ssb-pypitemplate
[file an issue]: https://github.com/statisticsnorway/dapla-toolbelt-pseudo/issues
[pip]: https://pip.pypa.io/

<!-- github-only -->

[license]: https://github.com/statisticsnorway/dapla-toolbelt-pseudo/blob/main/LICENSE
[contributor guide]: https://github.com/statisticsnorway/dapla-toolbelt-pseudo/blob/main/CONTRIBUTING.md
[reference guide]: https://statisticsnorway.github.io/dapla-toolbelt-pseudo/reference.html
