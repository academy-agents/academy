"""Academy home directory resolution."""

from __future__ import annotations

import os
import pathlib


def get_academy_home() -> pathlib.Path:
    """Resolve the Academy data directory.

    Resolves the Academy data directory from the `ACADEMY_HOME`
    environment variable if set, otherwise `~/local/share/academy`.
    Callers are responsible for creating the directory as needed.

    Returns:
        Path to the Academy data directory.
    """
    default = pathlib.Path.home() / 'local' / 'share' / 'academy'
    return pathlib.Path(os.environ.get('ACADEMY_HOME', default))
