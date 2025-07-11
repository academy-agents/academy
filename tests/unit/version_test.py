from __future__ import annotations

import academy


def test_package_version() -> None:
    assert isinstance(academy.__version__, str)
