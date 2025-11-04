from __future__ import annotations

import json
import logging
import pathlib

import pytest

from academy.logging import init_logging
from academy.logging import JSONHandler

# Note: these tests are just for coverage to make sure the code is functional.
# It does not test the agent of init_logging because pytest captures
# logging already.


@pytest.mark.parametrize(('color', 'extra'), ((True, True), (False, False)))
def test_logging_no_file(color: bool, extra: bool) -> None:
    init_logging(color=color, extra=extra)

    logger = logging.getLogger()
    logger.info('Test logging')


@pytest.mark.parametrize(
    ('color', 'extra'),
    ((True, True), (False, False), (False, 2)),
)
def test_logging_with_file(
    color: bool,
    extra: bool,
    tmp_path: pathlib.Path,
) -> None:
    filepath = tmp_path / 'log.txt'
    init_logging(logfile=filepath, color=color, extra=extra)

    logger = logging.getLogger()
    logger.info('Test logging')


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
