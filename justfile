# Print this help message.
help:
    @just --list

# Create a virtualenv and install all dependencies.
install:
    python3 -m venv .venv
    .venv/bin/pip install --upgrade pip
    .venv/bin/pip install -e '.[dev,docs]'
    pre-commit install

# Run the test suite against every supported python version.
test-all:
    tox run --color=yes

# Run the test suite against a single python minor version.
test pyversion='13':
    tox run --color=yes -e py3{{pyversion}}

# Run standard linting against the codebase.
lint:
  pre-commit run --all-files

# Remove generated build artifacts.
clean:
    rm -rf .venv
    rm -rf build dist *.egg-info
    rm -rf .coverage .coverage.* htmlcov coverage.xml
    rm -rf .tox .pytest_cache .mypy_cache
    find . -type d -name __pycache__ -exec rm -rf {} +
    find . \( -type d -name __pycache__ -or -name \*.py[oc] \) -delete
