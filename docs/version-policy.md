We follow [Semantic Versioning (SemVer)](https://semver.org/), with adjustments for pre-1.0 development.
Documentation is versioned to match the software and help users find information relevant to the version they are using.

## Versioning Rules

* `MAJOR.MINOR.PATCH` format
* While the project is in major version 0, breaking changes may occur in minor releases
* After 1.0, breaking changes will only occur in MAJOR versions

## Documentation Versions

Documentation is published under version-specific URLs such as:

* `/latest` and `/main` – Tracks the main branch with unreleased changes
* `/stable` – Tracks the most recent release version (default redirect)
* `/0.2.0` – Previous release version

## Deprecation Policy

We aim to provide advance notice before removing or changing functionality that may impact users.

**Before 1.0:** Because the project is in active development, features may be changed or removed in any minor release without formal deprecation.

**After 1.0:** Features will be marked as deprecated at least one MINOR version before being removed in a future MAJOR release.
Deprecations will be noted in the documentation and changelog.
Where applicable, documentation will offer guidance for migration or alternatives.
Deprecation warnings may be included in the user interface, API responses, logs, or CLI output depending on the component.
