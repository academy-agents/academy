from __future__ import annotations

import time
from datetime import timedelta
from typing import Any

import pytest

from academy.context import ActionContext
from academy.exchange import LocalExchangeTransport
from academy.exchange import UserExchangeClient
from academy.identifier import AgentId
from academy.identifier import UserId
from academy.stats import AgentStats
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


def test_agent_stats_lifetime_before_start() -> None:
    stats = AgentStats()
    assert stats.lifetime is None


def test_agent_stats_lifetime_after_start() -> None:
    stats = AgentStats()
    stats._start_time = time.monotonic()
    lifetime = stats.lifetime
    assert isinstance(lifetime, timedelta)
    assert lifetime.total_seconds() >= 0


def test_agent_stats_completed_messages_empty() -> None:
    stats = AgentStats()
    assert stats.completed_messages == {}


def test_agent_stats_completed_messages_incremented() -> None:
    source: AgentId[EmptyAgent] = AgentId.new()
    stats = AgentStats()
    stats.completed_messages[source] = (
        stats.completed_messages.get(source, 0) + 1
    )
    first_count = 1
    assert stats.completed_messages[source] == first_count
    stats.completed_messages[source] = (
        stats.completed_messages.get(source, 0) + 1
    )
    second_count = 2
    assert stats.completed_messages[source] == second_count


def test_agent_stats_completed_messages_multiple_sources() -> None:
    source_a: AgentId[EmptyAgent] = AgentId.new()
    source_b: AgentId[EmptyAgent] = AgentId.new()
    stats = AgentStats()
    count_a = 3
    count_b = 7
    stats.completed_messages[source_a] = count_a
    stats.completed_messages[source_b] = count_b
    assert stats.completed_messages[source_a] == count_a
    assert stats.completed_messages[source_b] == count_b


def test_agent_stats_inflight_messages_default() -> None:
    stats = AgentStats()
    assert stats.inflight_messages == 0


def test_agent_stats_inflight_messages_incremented() -> None:
    stats = AgentStats()
    stats.inflight_messages += 1
    assert stats.inflight_messages == 1
    stats.inflight_messages -= 1
    assert stats.inflight_messages == 0
