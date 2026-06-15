from __future__ import annotations

import os
import pathlib
from unittest import mock

from academy.home import get_academy_home


def test_get_academy_home_default(tmp_path: pathlib.Path) -> None:
    env = {'HOME': str(tmp_path)}
    with mock.patch.dict(os.environ, env, clear=True):
        expected = tmp_path / 'local' / 'share' / 'academy'
        assert get_academy_home() == expected


def test_get_academy_home_env_override(tmp_path: pathlib.Path) -> None:
    env = {'ACADEMY_HOME': str(tmp_path)}
    with mock.patch.dict(os.environ, env):
        assert get_academy_home() == tmp_path
