from __future__ import annotations

import logging
import multiprocessing
import uuid
from concurrent.futures import ProcessPoolExecutor

import pytest

from academy.agent import Agent
from academy.logging.recommended import recommended_logging
from academy.manager import Manager

logger = logging.getLogger(__name__)


class LogAgent(Agent):
    def __init__(self, s) -> None:
        super().__init__()
        self._s = s

    async def agent_on_startup(self) -> None:
        logger.info(f'Log agent proof-of-log token {self._s}')


@pytest.mark.asyncio
async def test_manager_configured_agent_logs(http_exchange_factory, tmp_path):
    spawn_context = multiprocessing.get_context('spawn')

    # arbitrary strings to log that we expect to not otherwise occur
    a_str = str(uuid.uuid4())
    b_str = str(uuid.uuid4())
    m_str = str(uuid.uuid4())
    m_ended_str = str(uuid.uuid4())

    m_filepath = str(tmp_path / f'{uuid.uuid4()!s}.log')

    # it is important to have max_workers=1 here so that both agents will
    # run inside the same process, and so share a single Python-level
    # logging system.
    async with await Manager.from_exchange_factory(
        http_exchange_factory,
        executors=ProcessPoolExecutor(
            max_workers=1,
            mp_context=spawn_context,
        ),
        log_config=recommended_logging(
            logfile=m_filepath,
            extra=2,
            level=logging.DEBUG,
        ),
    ) as manager:
        logger.info(f'Manager proof of log {m_str}')

        agent = LogAgent(a_str)
        handle = await manager.launch(
            agent,
        )
        await handle.shutdown()

        # Reaching this point doesn't mean that the agent has actually
        # shut down, but the other agent won't start (and so its log
        # configuration won't happen) until the first agent shuts down
        # because there's a single worker.

        agent = LogAgent(b_str)
        handle = await manager.launch(
            agent,
        )

        await handle.shutdown()

        await manager.wait((handle,))

        with open(m_filepath) as f:
            m_contents = f.read()

    logger.info(f'Manager proof of log {m_ended_str}')

    assert a_str in m_contents, f'A-log should be in log file {m_filepath}'
    assert b_str in m_contents, f'B-log should be in log file {m_filepath}'
    assert m_str in m_contents, (
        f'manager log should be in log file {m_filepath}'
    )
    assert m_ended_str not in m_contents, (
        f'manager ended log should not be in log file {m_filepath}'
    )
