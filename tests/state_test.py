from __future__ import annotations

import pathlib

import pytest

from academy.behavior import action
from academy.behavior import Behavior
from academy.state import FileState


class _StatefulBehavior(Behavior):
    def __init__(self, state_path: pathlib.Path) -> None:
        self.state_path = state_path

    async def on_setup(self) -> None:
        self.state: FileState[str] = FileState(self.state_path)

    async def on_shutdown(self) -> None:
        self.state.close()

    @action
    async def get_state(self, key: str) -> str:
        return self.state[key]

    @action
    async def modify_state(self, key: str, value: str) -> None:
        self.state[key] = value


@pytest.mark.asyncio
async def test_file_state(tmp_path: pathlib.Path) -> None:
    state_path = tmp_path / 'state.dbm'

    behavior = _StatefulBehavior(state_path)
    await behavior.on_setup()
    key, value = 'foo', 'bar'
    await behavior.modify_state(key, value)
    assert await behavior.get_state(key) == value
    await behavior.on_shutdown()

    behavior = _StatefulBehavior(state_path)
    await behavior.on_setup()
    assert await behavior.get_state(key) == value
    await behavior.on_shutdown()
