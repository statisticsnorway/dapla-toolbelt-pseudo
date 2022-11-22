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

## Usage

### Pseudonymize

```python
from dapla_pseudo import pseudonymize

pseudonymize(file_path="./data/personer.json", fields=["fnr", "fornavn"])
```

### Depseudonymize

```python
from dapla_pseudo import depseudonymize

depseudonymize(file_path="./data/personer.json", fields=["fnr", "fornavn"])
```

### Repseudonymize

```python
from dapla_pseudo import repseudonymize

repseudonymize(file_path="./data/personer_deid.json", fields=["fnr", "fornavn"], source_key="ssb-common-key-1", target_key="ssb-common-key-2")
```

## Requirements

- [Dapla Toolbelt](https://github.com/statisticsnorway/dapla-toolbelt)

## Installation

You can install _Pseudonymization extensions for Dapla Toolbelt_ via [pip] from [PyPI]:

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
