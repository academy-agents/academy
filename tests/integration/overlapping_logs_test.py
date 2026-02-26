from __future__ import annotations

import logging
import uuid

import pytest

from academy.logging.helpers import log_context
from academy.logging.recommended import recommended_logging

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_nested_log_configs(http_exchange_factory, tmp_path):
    """Test that nested initializations of same config do not interfere."""
    m_filepath = str(tmp_path / f'{uuid.uuid4()!s}.log')

    lc = recommended_logging(logfile=m_filepath, extra=2, level=logging.INFO)

    with log_context(lc):
        logger.info('inside outer log context')

        m1_str = str(uuid.uuid4())
        logger.info(m1_str)

        with log_context(lc):
            m2_str = str(uuid.uuid4())
            logger.info(m2_str)

        m3_str = str(uuid.uuid4())
        logger.info(m3_str)

    m4_str = str(uuid.uuid4())
    logger.info(m4_str)

    with open(m_filepath) as f:
        assert len([line for line in f if m1_str in line]) == 1, (
            f'uuid should appear once in logfile {m_filepath}'
        )
    with open(m_filepath) as f:
        assert len([line for line in f if m2_str in line]) == 1, (
            f'uuid should appear once in logfile {m_filepath}'
        )
    with open(m_filepath) as f:
        assert len([line for line in f if m3_str in line]) == 1, (
            f'uuid should appear once in logfile {m_filepath}'
        )
    with open(m_filepath) as f:
        assert len([line for line in f if m4_str in line]) == 0, (
            f'uuid should not appear in logfile {m_filepath}'
        )


@pytest.mark.asyncio
async def test_non_nested_log_configs(http_exchange_factory, tmp_path):
    """Test non-nested log config overlaps do not interfere."""
    m_filepath = str(tmp_path / f'{uuid.uuid4()!s}.log')

    lc = recommended_logging(logfile=m_filepath, extra=2, level=logging.INFO)

    ctx1 = log_context(lc)
    ctx2 = log_context(lc)

    ctx1.__enter__()
    # now we are in only ctx1

    logger.info('inside outer log context')

    m1_str = str(uuid.uuid4())
    logger.info(m1_str)

    ctx2.__enter__()
    # now we are in both ctx1 and ctx2

    m2_str = str(uuid.uuid4())
    logger.info(m2_str)

    ctx1.__exit__(None, None, None)
    # now we are in only ctx2

    m3_str = str(uuid.uuid4())
    logger.info(m3_str)

    ctx2.__exit__(None, None, None)
    # now we are in no context

    m4_str = str(uuid.uuid4())
    logger.info(m4_str)

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
    with open(m_filepath) as f:
        assert len([line for line in f if m4_str in line]) == 0, (
            f'uuid should not appear in logfile {m_filepath}'
        )
