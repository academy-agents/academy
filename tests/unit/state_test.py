from __future__ import annotations

import pathlib

import pytest

from academy.agent import action
from academy.agent import Agent
from academy.state import FileState


class _StatefulAgent(Agent):
    def __init__(self, state_path: pathlib.Path) -> None:
        self.state_path = state_path

    async def agent_on_startup(self) -> None:
        self.state: FileState[str] = FileState(self.state_path)

    async def agent_on_shutdown(self) -> None:
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

    agent = _StatefulAgent(state_path)
    await agent.agent_on_startup()
    key, value = 'foo', 'bar'
    await agent.modify_state(key, value)
    assert await agent.get_state(key) == value
    await agent.agent_on_shutdown()

    agent = _StatefulAgent(state_path)
    await agent.agent_on_startup()
    assert await agent.get_state(key) == value
    await agent.agent_on_shutdown()
