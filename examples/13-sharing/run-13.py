"""Access-control example: agent-wide + per-action groups + control groups.

Demonstrates Academy's two grant mechanisms and the closed-default rule.
``ComputeAgent`` has one decorated action (``restricted_compute``) and one
undecorated action (``public_data``). Because *any* action carries a
``sharing`` decorator, the agent runs in fine-grained (closed-default)
mode: the undecorated ``public_data`` is reachable only by the owner and
by any agent-wide ``access_groups``.

The agent is launched with::

    RuntimeConfig(access_groups={DATA_USERS}, control_groups={OPS_GROUP})

Group meanings on the *hosted* exchange (Globus / HTTP):

- ``DATA_USERS`` (access_groups): may call **every** action
- ``COMPUTE_GROUP`` (decorator sharing): may call ``restricted_compute`` only
- ``OPS_GROUP`` (control_groups): may shut the agent down, no action access
- Owner: full access (always)

This script runs against the **local** exchange, which is a fully trusted,
self-hosted environment: it does not authenticate callers or stamp group
memberships, so every action call below simply succeeds. The same agent and
config launched on the hosted exchange would enforce the matrix above. See
``docs/concepts/access-control.md`` and example ``12-globus-exchange`` for the
hosted setup.

Usage:
    PYTHONPATH=. python examples/13-sharing/run-13.py
"""

from __future__ import annotations

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor

from academy.agent import action
from academy.agent import Agent
from academy.exchange import LocalExchangeFactory
from academy.logging.recommended import recommended_logging
from academy.manager import Manager
from academy.runtime import RuntimeConfig

logger = logging.getLogger(__name__)

# Placeholder group UUIDs — replace with real Globus group IDs when
# deploying against the hosted exchange.
DATA_USERS = '00000000-0000-0000-0000-000000000001'
COMPUTE_GROUP = '00000000-0000-0000-0000-000000000002'
OPS_GROUP = '00000000-0000-0000-0000-000000000003'


class ComputeAgent(Agent):
    """Agent with one decorated and one undecorated action.

    Because ``restricted_compute`` carries a ``sharing`` decorator, the
    agent enters fine-grained (closed-default) mode: ``public_data``
    (undecorated) is owner-only unless widened by ``access_groups``.
    """

    @action(sharing=[COMPUTE_GROUP])
    async def restricted_compute(self, value: float) -> float:
        """Only ``COMPUTE_GROUP`` members (and the owner) may call this."""
        return value * 2

    @action
    async def public_data(self) -> str:
        """Undecorated: widened by ``access_groups``; owner always allowed."""
        return 'shared data'


async def main() -> int:
    config = RuntimeConfig(
        access_groups={DATA_USERS},
        control_groups={OPS_GROUP},
    )

    async with await Manager.from_exchange_factory(
        factory=LocalExchangeFactory(),
        executors=ThreadPoolExecutor(),
        log_config=recommended_logging(),
    ) as manager:
        # On the hosted exchange, `config` seeds the agent's mailbox
        # shares (access + control groups) and the runtime enforces the
        # access matrix. On the local exchange it is accepted but not
        # enforced.
        agent = await manager.launch(ComputeAgent, config=config)

        result = await agent.restricted_compute(21.0)
        logger.info('restricted_compute(21.0) -> %s', result)

        data = await agent.public_data()
        logger.info('public_data() -> %r', data)

        await agent.shutdown()

    return 0


if __name__ == '__main__':
    raise SystemExit(asyncio.run(main()))
