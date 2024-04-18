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

Other examples can also be viewed through notebook files for [pseudo](tests/pseudo_examples.ipynb) and [depseudo](tests/depseudo_examples.ipynb)

### Pseudonymize

```python
from dapla_pseudo import Pseudonymize
import polars as pl

file_path="data/personer.csv"
dtypes = {"fnr": pl.Utf8, "fornavn": pl.Utf8, "etternavn": pl.Utf8, "kjonn": pl.Categorical, "fodselsdato": pl.Utf8}
df = pl.read_csv(file_path, dtypes=dtypes) # Create DataFrame from file

# Example: Single field default encryption (DAEAD)
result_df = (
    Pseudonymize.from_polars(df)                   # Specify what dataframe to use
    .on_fields("fornavn")                          # Select the field to pseudonymize
    .with_default_encryption()                     # Select the pseudonymization algorithm to apply
    .run()                                         # Apply pseudonymization to the selected field
    .to_polars()                                   # Get the result as a polars dataframe
)

# Example: Multiple fields default encryption (DAEAD)
result_df = (
    Pseudonymize.from_polars(df)                   # Specify what dataframe to use
    .on_fields("fornavn", "etternavn")             # Select multiple fields to pseudonymize
    .with_default_encryption()                     # Select the pseudonymization algorithm to apply
    .run()                                         # Apply pseudonymization to the selected fields
    .to_polars()                                   # Get the result as a polars dataframe
)

# Example: Single field sid mapping and pseudonymization (FPE)
result_df = (
    Pseudonymize.from_polars(df)                   # Specify what dataframe to use
    .on_fields("fnr")                              # Select the field to pseudonymize
    .with_stable_id()                              # Map the selected field to stable id
    .run()                                         # Apply pseudonymization to the selected fields
    .to_polars()                                   # Get the result as a polars dataframe
)
```

The default encryption algorithm is DAEAD (Deterministic Authenticated Encryption with Associated Data). However, if the
field is a valid Norwegian personal identification number (fnr, dnr), the recommended way to pseudonymize is to use
the function `with_stable_id()` to convert the identification number to a stable ID (SID) prior to pseudonymization.
In that case, the pseudonymization algorithm is FPE (Format Preserving Encryption).

> [!IMPORTANT]
> FPE requires minimum two bytes/characters to perform encryption and minimum four bytes in case of Unicode.

If a field cannot be converted using the function `with_stable_id()` the default behaviour is to use the original value
as input to the FPE encryption function. However, this behaviour can be changed by supplying a `on_map_failure` argument
like this:

```python
from dapla_pseudo import Pseudonymize

# Example: Single field sid mapping and pseudonymization (FPE), unmatching SIDs will return Null
result_df = (
    Pseudonymize.from_polars(df)
    .on_fields("fnr")
    .with_stable_id(on_map_failure="RETURN_NULL")
    .run()
    .to_polars()
)
```


### Reading dataframes

Note that you may also use a Pandas DataFrame as an input or output, by exchanging `from_polars` with `from_pandas`
and `to_polars` with `to_pandas`. However, Pandas is much less performant, so take special care especially if your
dataset is large.

Example:

```python
# Example: Single field default encryption (DAEAD)
df_pandas = (
    Pseudonymize.from_pandas(df)                   # Specify what dataframe to use
    .on_fields("fornavn")                          # Select the field to pseudonymize
    .with_default_encryption()                     # Select the pseudonymization algorithm to apply
    .run()                                         # Apply pseudonymization to the selected field
    .to_pandas()                                   # Get the result as a polars dataframe
)
```


### Validate SID mapping

```python
from dapla_pseudo import Validator
import polars as pl

file_path="data/personer.csv"
dtypes = {"fnr": pl.Utf8, "fornavn": pl.Utf8, "etternavn": pl.Utf8, "kjonn": pl.Categorical, "fodselsdato": pl.Utf8}
df = pl.read_polars(file_path, dtypes=dtypes)

result = (
    Validator.from_polars(df)                   # Specify what dataframe to use
    .on_field("fnr")                            # Select the field to validate
    .validate_map_to_stable_id()                # Validate that all the field values can be mapped to a SID
)
# The resulting dataframe contains the field values that didn't have a corresponding SID
result.to_polars()
```

A `sid_snapshot_date` can also be specified to validate that the field values can be mapped to a SID at a specific date:

```python
from dapla_pseudo import Validator
import polars as pl

file_path="data/personer.csv"
dtypes = {"fnr": pl.Utf8, "fornavn": pl.Utf8, "etternavn": pl.Utf8, "kjonn": pl.Categorical, "fodselsdato": pl.Utf8}

df = pl.read_csv(file_path, dtypes=dtypes)

result = (
    Validator.from_polars(df)
    .on_field("fnr")
    .validate_map_to_stable_id(
        sid_snapshot_date="2023-08-29"
    )
)
# Show metadata about the validation (e.g. which version of the SID catalog was used)
result.metadata
# Show the field values that didn't have a corresponding SID
result.to_polars()
```

## Advanced usage

### Pseudonymize

#### Read from file systems

```python
from dapla_pseudo import Pseudonymize
from dapla import AuthClient


file_path="data/personer.csv"

options = {
    "dtypes": {"fnr": pl.Utf8, "fornavn": pl.Utf8, "etternavn": pl.Utf8, "kjonn": pl.Categorical, "fodselsdato": pl.Utf8}
}


# Example: Read DataFrame from file
result_df = (
    Pseudonymize.from_file(file_path)   # Read the data from file
    .on_fields("fornavn", "etternavn")  # Select multiple fields to pseudonymize
    .with_default_encryption()          # Select the pseudonymization algorithm to apply
    .run()                              # Apply pseudonymization to the selected fields
    .to_polars(**options)               # Get the result as a Pandas DataFrame
)

# Example: Read dataframe from GCS bucket
options = {
    "dtypes": {"fnr": pl.Utf8, "fornavn": pl.Utf8, "etternavn": pl.Utf8, "kjonn": pl.Categorical, "fodselsdato": pl.Utf8}
}

gcs_file_path = "gs://ssb-staging-dapla-felles-data-delt/felles/pseudo-examples/andeby_personer.csv"

result_df = (
    Pseudonymize.from_file(gcs_file_path)  # Read DataFrame from GCS
    .on_fields("fornavn", "etternavn")     # Select multiple fields to pseudonymize
    .with_default_encryption()             # Select the pseudonymization algorithm to apply
    .run()                                 # Apply pseudonymization to the selected fields
    .to_polars(**options)                  # Get the result as a polars dataframe
)
```

#### Pseudonymize using custom keys/keysets

```python
from dapla_pseudo import Pseudonymize, PseudoKeyset

# Pseudonymize fields in a local file using the default key:
df = (
    Pseudonymize.from_polars(df)                            # Specify what dataframe to use
    .on_fields("fornavn")                                   # Select the field to pseudonymize
    .with_default_encryption()                              # Select the pseudonymization algorithm to apply
    .run()                                         # Apply pseudonymization to the selected field
    .to_polars()                                            # Get the result as a polars dataframe
)

# Pseudonymize fields in a local file, explicitly denoting the key to use:
df = (
    Pseudonymize.from_polars(df)                            # Specify what dataframe to use
    .on_fields("fornavn")                                   # Select the field to pseudonymize
    .with_default_encryption(custom_key="ssb-common-key-2") # Select the pseudonymization algorithm to apply
    .run()                                         # Apply pseudonymization to the selected field
    .to_polars()                                            # Get the result as a polars dataframe
)

# Pseudonymize a local file using a custom keyset:
import json
custom_keyset = PseudoKeyset(
    encrypted_keyset="CiQAp91NBhLdknX3j9jF6vwhdyURaqcT9/M/iczV7fLn...8XYFKwxiwMtCzDT6QGzCCCM=",
    keyset_info={
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
    kek_uri="gcp-kms://projects/some-project-id/locations/europe-north1/keyRings/some-keyring/cryptoKeys/some-kek-1",
)

df = (
    Pseudonymize.from_polars(df)
    .on_fields("fornavn")
    .with_default_encryption(custom_key="1234567890") # Note that the custom key has to be the same as "primaryKeyId" in the custom keyset
    .run(custom_keyset=custom_keyset)
    .to_polars()
)
```
#### Pseudonymize using custom rules

Instead of declaring the pseudonymization rules via the Pseudonymize functions, one can define the rules manually.
This can be done via the `PseudoRule` class like this:

```python
from dapla_pseudo import Pseudonymize, PseudoRule

rule_json = {
    'name': 'my-fule',
     'pattern': '**/identifiers/*',
     'func': 'redact(placeholder=#)' # This is a shorthand representation of the redact function
}

rule = PseudoRule.from_json(rule_json)

df = (
    Pseudonymize.from_polars(df)
    .add_rules(rule) # Add to pseudonymization rules
    .run()
    .to_polars()
)
```

Pseudonymization rules can also be read from file. This is especially handy when there are several rules, or if you
prefer to store and maintain pseudonymization rules externally. For example:

```python
from dapla_pseudo import PseudoRule
import json

with open("pseudo-rules.json", 'r') as rules_file:
    rules_json = json.load(rules_file)

pseudo_rules = [PseudoRule.from_json(rule) for rule in rules_json]

df = (
    Pseudonymize.from_polars(df)
    .add_rules(pseudo_rules)
    .run()
    .to_polars()
)
```

### Depseudonymize

The "Depseudonymize" functions are almost exactly the same as when pseudonymizing.
User can map from Stable ID *back to* FNR.

```python
from dapla_pseudo import Depseudonymize
import polars as pl

file_path="data/personer_pseudonymized.csv"
dtypes = {"fnr": pl.Utf8, "fornavn": pl.Utf8, "etternavn": pl.Utf8, "kjonn": pl.Categorical, "fodselsdato": pl.Utf8}
df = pl.read_csv(file_path, dtypes=dtypes) # Create DataFrame from file

# Example: Single field default encryption (DAEAD)
result_df = (
    Depseudonymize.from_polars(df)                 # Specify what dataframe to use
    .on_fields("fornavn")                          # Select the field to depseudonymize
    .with_default_encryption()                     # Select the depseudonymization algorithm to apply
    .run()                                         # Apply depseudonymization to the selected field
    .to_polars()                                   # Get the result as a polars dataframe
)

# Example: Multiple fields default encryption (DAEAD)
result_df = (
    Depseudonymize.from_polars(df)                 # Specify what dataframe to use
    .on_fields("fornavn", "etternavn")             # Select multiple fields to depseudonymize
    .with_default_encryption()                     # Select the depseudonymization algorithm to apply
    .run()                                         # Apply depseudonymization to the selected fields
    .to_polars()                                   # Get the result as a polars dataframe
)

# Example: Depseudonymize Fnr field with SID mapping
result_df = (
    Depseudonymize.from_polars(df)                 # Specify what dataframe to use
    .on_fields("fnr")                              # Select fnr field to depseudonymize
    .with_stable_id()                              # Select the depseudonymization method (SID mapping) to apply
    .run()                                         # Apply depseudonymization to the selected fields
    .to_polars()                                   # Get the result as a polars dataframe
)

```


_Note that depseudonymization requires elevated access privileges._

### Repseudonymize
Repseudonymize can either 1) Change the algorithm used to pseudonymize, and/or 2)
change the key used in pseudonymization, while keeping the algorithm.

```python
# Example: Repseudonymize from PAPIS-compatible encryption to Stable ID
result_df = (
    Repseudonymize.from_polars(df)                 # Specify what dataframe to use
    .on_fields("fnr")                              # Select the field to pseudonymize
    .from_papis_compatible_encryption()            # Select the pseudonymization algorithm previously used
    .to_stable_id()                                # Select the new pseudonymization rule
    .run()                                         # Apply pseudonymization to the selected field
    .to_polars()                                   # Get the result as a polars dataframe
)
```

```python
# Example: Repseudonymize with the same algorithm, but with a different key
result_df = (
    Repseudonymize.from_polars(df)                     # Specify what dataframe to use
    .on_fields("fnr")                                  # Select the field to pseudonymize
    .from_papis_compatible_encryption()                # Select the pseudonymization algorithm previously used
    .to_papis_compatible_encryption(key_id="some-key") # Select the new pseudonymization rule
    .run()                                             # Apply pseudonymization to the selected field
    .to_polars()                                       # Get the result as a polars dataframe
)
```
### Datadoc

Datadoc metadata is gathered while pseudonymizing, and can be seen like so:

```python
result = (
    Pseudonymize.from_polars(df)
    .on_fields("fornavn")
    .with_default_encryption()
    .run()
)

print(result.datadoc)
```

Datadoc metadata is automatically written to the folder or bucket as the pseudonymized
data, when using the `to_file()` method on the result object.
The metadata file has the suffix `__DOC`, and is always a `.json` file.
The data and metadata is written to the file like so:

```python
result = (
    Pseudonymize.from_polars(df)
    .on_fields("fornavn")
    .with_default_encryption()
    .run()
)

# The line of code below also writes the file "gs://bucket/test__DOC.json"
result.to_file("gs://bucket/test.parquet")
```

Note that if you choose to only use the DataFrame from the result, **the metadata will be lost forever**!
An example of how this can happen:

```python
import dapla as dp
result = (
    Pseudonymize.from_polars(df)
    .on_fields("fornavn")
    .with_default_encryption()
    .run()
)
df = result.to_pandas()

dp.write_pandas(df, "gs://bucket/test.parquet", file_format="parquet") # The metadata is lost!!
```


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
