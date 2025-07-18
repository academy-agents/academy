from __future__ import annotations

from typing import Any

import pytest

from academy.context import ActionContext
from academy.exchange import LocalExchangeTransport
from academy.exchange import UserExchangeClient
from academy.identifier import AgentId
from academy.identifier import UserId
from testing.agents import EmptyAgent


@pytest.mark.asyncio
async def test_action_context_agent_source(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
) -> None:
    factory = exchange_client.factory()
    source_id: AgentId[EmptyAgent] = AgentId.new()
    registration = await exchange_client.register_agent(EmptyAgent)

    async def _request_handler(_: Any) -> None:  # pragma: no cover
        pass

    async with await factory.create_agent_client(
        registration,
        _request_handler,
    ) as agent_client:
        context = ActionContext(source_id, agent_client)
        assert context.is_agent_source()
        assert not context.is_user_source()
        handle = context.source_handle
        assert handle.agent_id == source_id
        # Check that handle.source_handle is cached
        assert context.source_handle is handle


@pytest.mark.asyncio
async def test_action_context_user_source(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
) -> None:
    factory = exchange_client.factory()
    source_id = UserId.new()
    registration = await exchange_client.register_agent(EmptyAgent)

    async def _request_handler(_: Any) -> None:  # pragma: no cover
        pass

    async with await factory.create_agent_client(
        registration,
        _request_handler,
    ) as agent_client:
        context = ActionContext(source_id, agent_client)
        assert not context.is_agent_source()
        assert context.is_user_source()

        with pytest.raises(TypeError):
            _ = context.source_handle
