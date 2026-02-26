from __future__ import annotations

import logging
import multiprocessing
import uuid
from concurrent.futures import ProcessPoolExecutor

import pytest

from academy.agent import Agent
from academy.logging.configs.file import FileLogging
from academy.manager import Manager

logger = logging.getLogger(__name__)


class LogAgent(Agent):
    def __init__(self, s) -> None:
        super().__init__()
        self._s = s

    async def agent_on_startup(self) -> None:
        logger.info(f'Log agent proof-of-log token {self._s}')


@pytest.mark.asyncio
async def test_sequential_agents(http_exchange_factory, tmp_path):
    spawn_context = multiprocessing.get_context('spawn')

    # arbitrary strings to log that we expect to not otherwise occur
    a_str = str(uuid.uuid4())
    b_str = str(uuid.uuid4())

    # it is important to have max_workers=1 here so that both agents will
    # run inside the same process, and so share a single Python-level
    # logging system.
    async with await Manager.from_exchange_factory(
        http_exchange_factory,
        executors=ProcessPoolExecutor(
            max_workers=1,
            mp_context=spawn_context,
        ),
    ) as manager:
        a_filepath = str(tmp_path / f'{uuid.uuid4()!s}.log')
        a_lc = FileLogging(logfile=a_filepath, level=logging.INFO)
        agent = LogAgent(a_str)
        handle = await manager.launch(
            agent,
            log_config=a_lc,
        )
        await handle.shutdown()

        # Reaching this point doesn't mean that the agent has actually
        # shut down, but the other agent won't start (and so its log
        # configuration won't happen) until the first agent shuts down
        # because there's a single worker.

        b_filepath = str(tmp_path / f'{uuid.uuid4()!s}.log')
        b_lc = FileLogging(logfile=b_filepath, level=logging.INFO)
        agent = LogAgent(b_str)
        handle = await manager.launch(
            agent,
            log_config=b_lc,
        )

        await handle.shutdown()

        await manager.wait((handle,))

        with open(a_filepath) as f:
            a_contents = f.read()
        with open(b_filepath) as f:
            b_contents = f.read()

        assert a_str in a_contents, (
            f'A-log should be in first log file {a_filepath}'
        )
        assert a_str not in b_contents, (
            f'A-log should not be in second log file {b_filepath}'
        )

        assert b_str not in a_contents, (
            f'B-log should not be in first log file {a_filepath}'
        )
        assert b_str in b_contents, (
            f'B-log should be in second log file {b_filepath}'
        )
