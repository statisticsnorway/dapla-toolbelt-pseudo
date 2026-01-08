# Contributor Guide

Thank you for your interest in improving this project.
This project is open-source under the [{{cookiecutter.license.replace("-", " ")}} license] and
welcomes contributions in the form of bug reports, feature requests, and pull requests.

Here is a list of important resources for contributors:

- [Source Code]
- [Documentation]
- [Issue Tracker]
- [Code of Conduct]

## How to report a bug

Report bugs on the [Issue Tracker].

When filing an issue, make sure to answer these questions:

- Which operating system and Python version are you using?
- Which version of this project are you using?
- What did you do?
- What did you expect to see?
- What did you see instead?

The best way to get your bug fixed is to provide a test case,
and/or steps to reproduce the issue.

## How to request a feature

Request features on the [Issue Tracker].

## How to set up your development environment

You need Python 3.10+ and the following tools:

- [uv]

Install [pipx]:

```console
python -m pip install --user pipx
python -m pipx ensurepath
```

Install [Uv]:

```console
pipx install uv
```

Install the pre-commit hooks

```console
nox --session=pre-commit -- install
```

Install the package with development requirements:

```console
uv sync
```

You can now run an interactive Python session, or your app:

```console
uv run python
uv run dapla-toolbelt-pseudo
```

## How to test the project

Run the full test suite:

```console
uvx nox
```

List the available Nox sessions:

```console
uvx nox --list-sessions
```

You can also run a specific Nox session.
For example, invoke the unit test suite like this:

```console
uvx nox --session=tests
```

Unit tests are located in the _tests_ directory,
and are written using the [pytest] testing framework.

Integration tests are located in _tests/integration_,
and require the `@integration_test` decorator and `setup` fixture for environment-specific configurations.
To run the tests locally, the user must be part of the `pseudo-service-admin-t@ssb.no` group.

## How to submit changes

Open a [pull request] to submit changes to this project.

Your pull request needs to meet the following guidelines for acceptance:

- The Nox test suite must pass without errors and warnings.
- Include unit tests. This project maintains 100% code coverage.
- If your changes add functionality, update the documentation accordingly.

Feel free to submit early, though—we can always iterate on this.

To run linting and code formatting checks before committing your change, you can install pre-commit as a Git hook by running the following command:

```console
nox --session=pre-commit -- install
```

It is recommended to open an issue before starting work on anything.
This will allow a chance to talk it over with the owners and validate your approach.

[mit license]: https://opensource.org/licenses/MIT
[source code]: https://github.com/statisticsnorway/dapla-toolbelt-pseudo
[documentation]: https://statisticsnorway.github.io/dapla-toolbelt-pseudo
[issue tracker]: https://github.com/statisticsnorway/dapla-toolbelt-pseudo/issues
[pipx]: https://pipx.pypa.io/
[uv]: https://docs.astral.sh/uv/
[nox]: https://nox.thea.codes/
[pytest]: https://pytest.readthedocs.io/
[pull request]: https://github.com/statisticsnorway/dapla-toolbelt-pseudo/pulls

<!-- github-only -->

[code of conduct]: CODE_OF_CONDUCT.md
