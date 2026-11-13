# Print this help message.
help:
    @just --list

# Create a virtualenv and install all dependencies.
install:
    python3 -m venv .venv
    .venv/bin/pip install --upgrade pip
    .venv/bin/pip install -e '.[dev,docs]'
    pre-commit install

# Run the test suite; pass a python minor version (e.g. `just test 13`) to test only that version.
test pyversion='':
    tox run --color=yes {{ if pyversion != '' { '-e py3' + pyversion } else { '' } }}

# Remove generated build artifacts.
clean:
    rm -rf .venv
    rm -rf build dist *.egg-info
    rm -rf .coverage .coverage.* htmlcov coverage.xml
    rm -rf .tox .pytest_cache .mypy_cache
    find . -type d -name __pycache__ -exec rm -rf {} +
    find . \( -type d -name __pycache__ -or -name \*.py[oc] \) -delete
