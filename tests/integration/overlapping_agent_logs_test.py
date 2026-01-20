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

import logging
import uuid

import pytest

from academy.logging import log_context
from academy.logging.recommended import recommended_logging2

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_nested_log_configs(http_exchange_factory, tmp_path):
    # to test:
    # start a context with a log config
    # check log appears once
    # start another context with the same config: the same config object.
    # check log appears once (I expect it will appear twice right now)
    # close one context
    # check log appears once
    # close final context
    # check log does not appear

    # TODO: again with non-nested blocks (start A, start B, end A, end B)
    # with 1-log tests at each stage

    # arbitrary strings to log that we expect to not otherwise occur

    m_filepath = str(tmp_path / f'{uuid.uuid4()!s}.log')

    # TODO: this init_logging should be undone at end of test -- otherwise
    # it's going to stick around for the rest of the log run. which suggests
    # that a log config could be a context manager. which is another argument
    # for more management around a log config perhaps as a super class
    # or some other way -- the superclass counter-argument is that
    # configs should be serializable, so keeping them simple is a good
    # thing to do. In which case, a log manager wrapper?

    lc = recommended_logging2(logfile=m_filepath, extra=2, level=logging.INFO)

    with log_context(lc):
        logger.info('inside outer log context')

        m1_str = str(uuid.uuid4())
        logger.info(m1_str)

        with log_context(lc):
            m2_str = str(uuid.uuid4())
            logger.info(m2_str)

        m3_str = str(uuid.uuid4())
        logger.info(m3_str)

    with open(m_filepath) as f:
        assert len([line for line in f if m1_str in line]) == 1, (
            f'uuid should appear once in logfile {m_filepath}'
        )
    with open(m_filepath) as f:
        assert len([line for line in f if m3_str in line]) == 1, (
            f'uuid should appear once in logfile {m_filepath}'
        )
    with open(m_filepath) as f:
        assert len([line for line in f if m2_str in line]) == 1, (
            f'uuid should appear once in logfile {m_filepath}'
        )
