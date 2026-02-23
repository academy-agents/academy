# This comes from a quote from Alok in github
# https://github.com/academy-agents/academy/pull/295#discussion_r2604534531
# that I am not satisfied is true and would like tested:
# init_logging multiple times per htex worker (or per process pool worker)
# is not much of an issue because they can override each other (i.e. the
# new agent running in that worker supplants anything else running in that
# worker, so the old thing doesn't log any more).

# TODO:
# make a process worker pool.
# run an agent in it.
# log (to a run-the-agent configured logfile)
# end the agent
# run a new agent in it, with different log config
# log (to a different logfile)
# assert that the log lines went to the right place
from __future__ import annotations

import asyncio
import logging
import multiprocessing
import uuid
from concurrent.futures import ProcessPoolExecutor

import pytest

from academy.agent import Agent
from academy.logging import log_context
from academy.logging.configs.file import FileLogging
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
async def test_benc(http_exchange_factory, tmp_path):
    spawn_context = multiprocessing.get_context('spawn')

    # arbitrary strings to log that we expect to not otherwise occur
    a_str = str(uuid.uuid4())
    b_str = str(uuid.uuid4())

    m_filepath = str(tmp_path / f'{uuid.uuid4()!s}.log')

    with log_context(
        recommended_logging(logfile=m_filepath, extra=2, level=logging.DEBUG),
    ):
        async with await Manager.from_exchange_factory(
            http_exchange_factory,
            executors=ProcessPoolExecutor(
                max_workers=1,
                mp_context=spawn_context,
            ),
        ) as manager:
            logger.info('Inside manager, step 1')

            a_filepath = str(tmp_path / f'{uuid.uuid4()!s}.log')
            a_lc = FileLogging(logfile=a_filepath, level=logging.INFO)
            agent = LogAgent(a_str)
            logger.info('prelaunch A')
            handle = await manager.launch(
                agent,
                log_config=a_lc,
            )
            await handle.shutdown()
            logger.info('after A shutdown')

            # does this returning ^ mean that all shutdown activity (eg log
            # removal in future dev) has happened?

            b_filepath = str(tmp_path / f'{uuid.uuid4()!s}.log')
            b_lc = FileLogging(logfile=b_filepath, level=logging.INFO)
            agent = LogAgent(b_str)
            handle = await manager.launch(
                agent,
                log_config=b_lc,
            )
            await handle.shutdown()

            # there's a race condition here on the log lines appearing
            # because they aren't (apparently) written synchronously
            await asyncio.sleep(10)

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
