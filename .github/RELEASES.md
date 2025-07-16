# Release Guide

1. Choose the next version number, referred to as `{VERSION}` for the
   rest of the instructions. Versioning follows semver
   (`major.minor.patch`) with optional [PEP-440](https://peps.python.org/pep-0440){target=_blank}
   pre-release/post-release/dev-release segments. Major/minor/patch numbers
   start at 0 and pre-release/post-release/dev-release segments start at 1.
2. Update the version in `pyproject.toml` to `{VERSION}`.
3. Commit and merge the version updates/changelogs into main.
4. Pull the latest changes to main, tag the commit updating the version, and push the tag.
   ```bash
   $ git tag -s v{VERSION} -m "Academy v{VERSION}"
   $ git push origin v{VERSION}
   ```
   Note the version number is prepended by "v" for the tags so we can
   distinguish release tags from non-release tags.
5. Create a new release on GitHub using the tag. The title should be
   `Academy v{VERSION}`.
6. **Official release:**
    1. Use the "Generate release notes" option and set the previous tag as the previous official release tag. E.g., for `v0.4.1`, the previous release tag should be `v0.4.0` and NOT `v0.4.1a1`.
    2. Add an "Upgrade Steps" section at the top (see previous releases for examples).
    3. Review the generated notes and edit as needed. PRs are organized by tag, but some PRs will be missing tags and need to be moved from the "Other Changes" section to the correct section.
    4. Select "Set as the latest release."
7. **Unofficial release:** (alpha/dev builds)
    1. Do NOT generate release notes. The body can be along the lines of "Development pre-prelease for `v{VERSION}`."
    2. Leave the previous tag as "auto."
    3. Select "Set as a pre-release."
