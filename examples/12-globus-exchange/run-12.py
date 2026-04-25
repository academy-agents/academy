"""Globus exchange example.

Uses GlobusExchangeFactory against the hosted exchange at
exchange.academy-agents.org. Each agent gets its own Globus Auth
client, credentials, and delegated token.

Agents are launched inside ``async with manager.launch_batch() as
batch:``, which coalesces all of the per-agent auth flows into a
single consent prompt. This is ergonomic sugar over the underlying
``manager.register_agents(...)`` + ``manager.launch(registration=...)``
pattern, which is still supported for pre-warming or register-elsewhere
workflows.

Requires:
    export ACADEMY_TEST_PROJECT_ID=<project-uuid>

Usage:
    PYTHONPATH=. python examples/12-globus-exchange/run-12.py
"""

from __future__ import annotations

import asyncio
import logging
import os
import uuid

from academy.agent import action
from academy.agent import Agent
from academy.exchange.cloud.globus import GlobusExchangeFactory
from academy.handle import Handle
from academy.logging.recommended import recommended_logging
from academy.manager import Manager

logger = logging.getLogger(__name__)


class Greeter(Agent):
    """Agent that greets people."""

    @action
    async def greet(self, name: str) -> str:
        greeting = f'Hello, {name}!'
        logger.info(f'Greeter: {greeting!r}')
        return greeting


class Shouter(Agent):
    """Agent that uppercases text."""

    @action
    async def shout(self, text: str) -> str:
        result = text.upper()
        logger.info(f'Shouter: {result!r}')
        return result


class Coordinator(Agent):
    """Orchestrates Greeter and Shouter."""

    def __init__(
        self,
        greeter: Handle[Greeter],
        shouter: Handle[Shouter],
    ) -> None:
        super().__init__()
        self.greeter = greeter
        self.shouter = shouter

    @action
    async def greet_loudly(self, name: str) -> str:
        greeting = await self.greeter.greet(name)
        return await self.shouter.shout(greeting)


async def main() -> int:

    project_id = uuid.UUID(os.environ['ACADEMY_TEST_PROJECT_ID'])

    factory = GlobusExchangeFactory(project_id=project_id)

    async with await Manager.from_exchange_factory(
        factory=factory,
        log_config=recommended_logging(),
    ) as manager:
        async with manager.launch_batch() as batch:
            greeter = await batch.launch(Greeter)
            shouter = await batch.launch(Shouter)
            coordinator = await batch.launch(
                Coordinator,
                args=(greeter, shouter),
            )

        result = await coordinator.greet_loudly('Academy')
        logger.info(f'Result: {result!r}')
        assert result == 'HELLO, ACADEMY!'

    return 0


if __name__ == '__main__':
    raise SystemExit(asyncio.run(main()))
