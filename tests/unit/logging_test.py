from __future__ import annotations

import json
import logging
import pathlib

import pytest

from academy.logging import init_logging
from academy.logging.config import ObservabilityConfig
from academy.logging.configs.console import ConsoleLogging
from academy.logging.configs.file import FileLogging
from academy.logging.configs.jsonpool import FilePoolLog
from academy.logging.helpers import JSONHandler

# TODO: reexamine this commentary in the context of new logging
# Note: these tests are just for coverage to make sure the code is functional.
# It does not test the agent of init_logging because pytest captures
# logging already.


@pytest.mark.parametrize('logfile', (None, 'test.txt'))
def test_init_logging(logfile) -> None:
    lc = init_logging(logfile=logfile)
    assert isinstance(lc, ObservabilityConfig)

    logger = logging.getLogger()
    logger.info('Test logging')

    # TODO: what to assert here?


@pytest.mark.parametrize(('color', 'extra'), ((True, True), (False, False)))
def test_console_logging(color: bool, extra: bool) -> None:
    lc = ConsoleLogging(color=color, extra=extra)
    lc.init_logging()

    logger = logging.getLogger()
    logger.info('Test logging')


@pytest.mark.parametrize(
    'extra',
    (False, True, 2),
)
def test_logging_with_file(
    tmp_path: pathlib.Path,
    extra: bool,
) -> None:
    _filepath = tmp_path / 'log.txt'
    assert isinstance(extra, int)
    lc = FileLogging(logfile=_filepath, extra=extra)
    lc.init_logging()

    logger = logging.getLogger()
    logger.info('Test logging')

    # TODO: assert the file exists and the string "Test logging" appears in it.


def test_logging_with_filepool() -> None:
    # TODO: what sort of path override makes sense here? for users and
    # for testing? for testing, to keep test files out of  ~/.academy
    # _filepath = tmp_path / 'log.txt'
    lc = FilePoolLog()
    lc.init_logging()

    logger = logging.getLogger()
    logger.info('Test logging')

    path = pathlib.Path.home() / '.academy' / 'logs' / lc._pool_uuid

    files = list(path.iterdir())
    assert len(files) == 1, (
        'There should be one log file in the pool directory'
    )

    # TODO: assert the file exists and the string "Test logging" appears in it.


def test_json_handler_emit(tmp_path: pathlib.Path) -> None:
    log_file = tmp_path / 'test.jsonl'
    handler = JSONHandler(log_file)

    record = logging.LogRecord(
        name='test_logger',
        level=logging.INFO,
        pathname=__file__,
        lineno=42,
        msg='Hello, world!',
        args=(),
        exc_info=None,
    )
    # Options passed to extra= are added to the __dict__ of LogRecord
    record.foo = 'bar'

    # Attach a formatter for the `formatted` attribute
    formatter = logging.Formatter('%(levelname)s: %(message)s')
    handler.setFormatter(formatter)

    handler.emit(record)

    contents = log_file.read_text().strip().splitlines()
    assert len(contents) == 1
    data = json.loads(contents[0])
    assert isinstance(data, dict)
    assert data['formatted'] == 'INFO: Hello, world!'
    assert data['msg'] == 'Hello, world!'
    assert data['levelname'] == 'INFO'
    assert data['lineno'] == '42'
    assert data['name'] == 'test_logger'
    assert data['foo'] == 'bar'

    handler.f.close()


def test_json_handler_emit_unrepresentable(tmp_path: pathlib.Path) -> None:
    log_file = tmp_path / 'test_bad.jsonl'
    handler = JSONHandler(log_file)

    class Bad:
        def __str__(self):
            raise ValueError('Cannot be converted to a str.')

    record = logging.LogRecord(
        name='test_logger',
        level=logging.WARNING,
        pathname=__file__,
        lineno=99,
        msg='This will break',
        args=(),
        exc_info=None,
    )
    record.bad = Bad()

    handler.emit(record)
    data = json.loads(log_file.read_text().strip())
    assert 'bad' in data
    assert 'Unrepresentable' in data['bad']

    handler.f.close()
