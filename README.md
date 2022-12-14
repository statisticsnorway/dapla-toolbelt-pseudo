# Pseudonymization extensions for Dapla Toolbelt

[![PyPI](https://img.shields.io/pypi/v/dapla-toolbelt-pseudo.svg)][pypi_]
[![Status](https://img.shields.io/pypi/status/dapla-toolbelt-pseudo.svg)][status]
[![Python Version](https://img.shields.io/pypi/pyversions/dapla-toolbelt-pseudo)][python version]
[![License](https://img.shields.io/pypi/l/dapla-toolbelt-pseudo)][license]

[![Tests](https://github.com/statisticsnorway/dapla-toolbelt-pseudo/workflows/Tests/badge.svg)][tests]
[![Codecov](https://codecov.io/gh/statisticsnorway/dapla-toolbelt-pseudo/branch/main/graph/badge.svg)][codecov]

[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)][pre-commit]
[![Black](https://img.shields.io/badge/code%20style-black-000000.svg)][black]

[pypi_]: https://pypi.org/project/dapla-toolbelt-pseudo/
[status]: https://pypi.org/project/dapla-toolbelt-pseudo/
[python version]: https://pypi.org/project/dapla-toolbelt-pseudo
[tests]: https://github.com/statisticsnorway/dapla-toolbelt-pseudo/actions?workflow=Tests
[codecov]: https://app.codecov.io/gh/statisticsnorway/dapla-toolbelt-pseudo
[pre-commit]: https://github.com/pre-commit/pre-commit
[black]: https://github.com/psf/black

Pseudonymize, repseudonymize and depseudonymize data on Dapla.

### Pseudonymize

```python
from dapla_pseudo import pseudonymize

# Pseudonymize fields in a local file using the default key:
pseudonymize(file_path="./data/personer.json", fields=["fnr", "fornavn"])

# Pseudonymize fields in a local file, explicitly denoting the key to use:
pseudonymize(file_path="./data/personer.json", fields=["fnr", "fornavn"], key="ssb-common-key-1")

# Pseudonymize a local file using a custom key:
import json
custom_keyset = json.dumps(    {
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
pseudonymize(file_path="./data/personer.json", fields=["fornavn"], sid=["fnr"])
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

- [Dapla Toolbelt](https://github.com/statisticsnorway/dapla-toolbelt)

## Installation

You can install _dapla-toolbelt-pseudo_ via [pip] from [PyPI]:

```console
$ pip install dapla-toolbelt-pseudo
```

## Contributing

Contributions are very welcome.
To learn more, see the [Contributor Guide].

## License

Distributed under the terms of the [MIT license][license],
_Pseudonymization extensions for Dapla Toolbelt_ is free and open source software.

## Issues

If you encounter any problems,
please [file an issue] along with a detailed description.

## Credits

This project was generated from [@cjolowicz]'s [Hypermodern Python Cookiecutter] template.

[@cjolowicz]: https://github.com/cjolowicz
[pypi]: https://pypi.org/
[hypermodern python cookiecutter]: https://github.com/cjolowicz/cookiecutter-hypermodern-python
[file an issue]: https://github.com/statisticsnorway/dapla-toolbelt-pseudo/issues
[pip]: https://pip.pypa.io/

<!-- github-only -->

[license]: https://github.com/statisticsnorway/dapla-toolbelt-pseudo/blob/main/LICENSE
[contributor guide]: https://github.com/statisticsnorway/dapla-toolbelt-pseudo/blob/main/CONTRIBUTING.md
[command-line reference]: https://statisticsnorway.github.io/dapla-toolbelt-pseudo
