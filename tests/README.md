# Test Suite

This directory contains all automated tests for the project, organized into two main categories: **unit tests** and **integration tests**.

We follow a clean separation between these types to ensure fast feedback during development and reliable end-to-end validation during continuous integration.

---

## Unit Tests (`tests/unit/`)

Unit tests are small, fast, and focused. They are used to verify the behavior of individual components in isolation.

**Characteristics**

* Test a single class, function, or module in isolation
* Use mocks or fakes to replace dependencies
* Run fast and do not require external services
* Should be the default place to add new tests

**Running**

All of the following commands run the same set of tests.
The default tox environments run only unit tests and check coverage appropriately.
```bash
pytest tests/unit
pytest -k "not integration"
tox -e py313
```

---

## Integration Tests (`tests/integration/`)

Integration tests validate the behavior of the system as a whole by interacting only with the public API of the codebase—just like an external user would.

**Characteristics**

* Each test file represents a single user-facing scenario
* Tests use only public interfaces—no mocking or private API access
* May depend on real services (e.g., database, API server)
* Slower, but catch issues unit tests can’t

**Running**

All of the following commands run the same set of tests.
The `*-integration` tox environments run only integration tests and checks coverage on only the integration tests themselves.
```bash
pytest tests/integration
pytest -k "integration"
tox -e py313-integration
```
