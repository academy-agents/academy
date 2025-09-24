We welcome contributions to Academy!
This guide covers all of the basics required to get started contributing to the project.

## Issues


We use GitHub issues to report problems, request and track changes, and discuss future ideas.
Please use the [Issue Tracker](https://github.com/academy-agents/academy/issues){target=_blank} and follow the template.

## Pull Requests

Please create an issue to discuss changes before creating pull requests, unless the change is for something trivial like a typo or docs change.

We use the standard GitHub contribution cycle where all contributions are made via pull requests (including code owners!).

1. Fork the repository and clone to your local machine.
1. Create a branch for your changes using a descriptive name for the change, such as the issue being addressed.
   ```
   git checkout -b {user}-issue-{xx}
   ```
1. Create your changes.
    - Changes should conform to the style and testing guidelines, described below.
    - Keep commits focused and use clear messages.
    - Avoid committing unrelated changes in the same PR.
1. Test your changes.
1. Commit your changes and push your branch.
1. Open a pull request in this repository, fill out the PR template, and link any relevant issues.

## Developing

### Installation and Setup

You will need a supported version of Python and git to get started for local development.

First, fork the repository on GitHub and clone your fork locally.
```bash
$ git clone https://github.com/<USER>/academy
$ cd academy
```

Then, create a virtual environment and install the development and documentation dependencies.
```bash
$ python -m venv venv
$ . venv/bin/activate
$ pip install -e .[dev,docs]
```

### Linting and Type Checking

We use pre-commit to run linters and static type checkers.
Install the pre-commit hook and run against files:
```bash
$ pre-commit install
$ pre-commit run --all-files
```

### Testing

The entire CI workflow can be run with `#!bash $ tox`.
This will test against multiple versions of Python and can be slow.

Module-level unit-test are located in the `tests/unit` directory and its structure is intended to match that of `academy/`.
E.g. the tests for `academy/x/y.py` are located in `tests/unit/x/y_test.py`; however, additional test files can be added as needed.
Tests should be narrowly focused and target a single aspect of the code's functionality, tests should not test internal implementation details of the code, and tests should not be dependent on the order in which they are run.

```bash
# Run all unit tests
$ tox -e py313
# Run a specific unit test
$ tox -e py313 -- tests/unit/x/y_test.py::test_z
```

Code that is useful for building tests but is not a test itself belongs in the `testing/` directory.

Integration tests are located in `tests/integration`, and each file contains one integration test.

```bash
# Run all integration testts
$ tox -e py313-integration
```

### Docs

If code changes require an update to the documentation (e.g., for function signature changes, new modules, etc.), the documentation can be built using MKDocs.

```bash
# Manually
$ pip install -e .[docs] # If you skipped this step earlier
$ mkdocs build --strict  # Build only to site/index.html
$ mkdocs serve           # Serve locally

# With tox (will only build, does not serve)
$ tox -e docs
```

### Style Guide

The Python code and docstring format mostly follows Google's [Python Style Guide](https://google.github.io/styleguide/pyguide.html){target=_blank}, but the pre-commit config is the authoritative source for code format compliance.

**Tips:**

* Avoid redundant comments---write _why_ and not _what_.
* Keep comments and docstrings up-to-date when changing functions and classes.
* Don't include unrelated formatting or refactors in a feature PR.
* Prefer pure functions where possible.
* Define all class attributes inside `__init__` so all attributes are visible in one place.
  Attributes that are defined later can be set as `None` as a placeholder.
* Prefer f-strings (`#!python f'name: {name}`) over string format (`#!python 'name: {}'.format(name)`).
  Never use the `%` operator.
* Prefer [typing.NamedTuple][] over [collections.namedtuple][].
* Use sentence case for error and log messages, but only include punctuation for errors.
  ```python
  logger.info(f'new connection opened to {address}')
  raise ValueError('Name must contain alphanumeric characters only.')
  ```
* Document all exceptions that may be raised by a function in the docstring.
