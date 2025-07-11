from __future__ import annotations

import logging
import pathlib

import pytest

from academy.logging import init_logging

# Note: these tests are just for coverage to make sure the code is functional.
# It does not test the agent of init_logging because pytest captures
# logging already.


@pytest.mark.parametrize(('color', 'extra'), ((True, True), (False, False)))
def test_logging_no_file(color: bool, extra: bool) -> None:
    init_logging(color=color, extra=extra)

    logger = logging.getLogger()
    logger.info('Test logging')


@pytest.mark.parametrize(('color', 'extra'), ((True, True), (False, False)))
def test_logging_with_file(
    color: bool,
    extra: bool,
    tmp_path: pathlib.Path,
) -> None:
    filepath = tmp_path / 'log.txt'
    init_logging(logfile=filepath, color=color, extra=extra)

    logger = logging.getLogger()
    logger.info('Test logging')
