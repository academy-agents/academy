from __future__ import annotations

import json
import logging
import pathlib
import uuid
from unittest import mock

import pytest

from academy.logging import initialized_log_contexts
from academy.logging import log_context
from academy.logging.config import LogConfig
from academy.logging.configs.console import ConsoleLogging
from academy.logging.configs.file import FileLogging
from academy.logging.configs.jsonpool import JSONPoolLogging
from academy.logging.configs.multi import MultiLogging
from academy.logging.helpers import JSONHandler
from academy.logging.recommended import recommended_logging


@pytest.mark.parametrize(('color', 'extra'), ((True, True), (False, False)))
def test_console_logging(color: bool, extra: bool, capteesys) -> None:
    lc = ConsoleLogging(color=color, extra=extra)
    with log_context(lc):
        logger = logging.getLogger()
        logger.info('Test logging')
        assert 'Test logging' in capteesys.readouterr().out


def test_none_logging() -> None:
    with log_context(None):
        logger = logging.getLogger()
        logger.info('Test logging')
        # Nothing to assert about where this log message went...
        # just testing that we can go into the log_context with no config


def test_recommended_logging(capteesys, tmp_path) -> None:
    logfile = tmp_path / 'log'
    with log_context(recommended_logging(logfile=logfile)):
        logger = logging.getLogger()
        logger.info('Test logging')
        assert 'Test logging' in capteesys.readouterr().out
        with open(logfile) as f:
            assert 'Test logging' in f.read()


def test_nested_context() -> None:
    """This tests that nesting/reference counting happens based on string ID.

    The behaviour under test is that a configuration may be defined in one
    process (e.g. workflow submit process) and then be conveyed using pickle
    multiple times to a destination process, such as a Parsl worker process,
    resulting in two distinct objects for the same configuration, and that
    configuration should be initialized only once.
    """
    lc = mock.Mock()
    assert lc.uuid not in initialized_log_contexts, (
        'lc should not be in a context yet'
    )
    with log_context(lc):
        assert initialized_log_contexts[lc.uuid] == 1, (
            'lc should have one reference'
        )
        with log_context(lc):
            # PLR2004 Magic value used in comparison
            # This is a lexical property of the surrounding code, the nesting
            # depth.
            assert initialized_log_contexts[lc.uuid] == 2, (  # noqa: PLR2004
                'lc should have two references'
            )
        assert initialized_log_contexts[lc.uuid] == 1, (
            'lc should have one reference after end of one with block'
        )
    assert lc.uuid not in initialized_log_contexts, (
        'lc should not be in a context after all with blocks exited'
    )


def test_nested_context_same_uuid_different_object() -> None:
    """This tests that nesting/reference counting happens based on uuid.

    The behaviour under test is that a configuration may be defined in one
    process (e.g. workflow submit process) and then be conveyed using pickle
    multiple times to a destination process, such as a Parsl worker process,
    resulting in two distinct objects for the same configuration, and that
    configuration should be initialized only once.
    """
    u = str(uuid.uuid4())
    lc1 = mock.Mock(LogConfig)
    lc1.uuid = u
    lc2 = mock.Mock(LogConfig)
    lc2.uuid = u

    assert u not in initialized_log_contexts, (
        'config should not be in a context yet'
    )

    lc1.init_logging.assert_not_called()
    with log_context(lc1):
        lc1.init_logging.assert_called_once()
        assert initialized_log_contexts[u] == 1, (
            'config should have one reference'
        )
        with log_context(lc2):
            # PLR2004 Magic value used in comparison
            # This is a lexical property of the surrounding code, the nesting
            # depth.
            assert initialized_log_contexts[u] == 2, (  # noqa: PLR2004
                'config should have two references'
            )
        assert initialized_log_contexts[u] == 1, (
            'config should have one reference after end of one with block'
        )
    assert u not in initialized_log_contexts, (
        'config should not be in a context after all with blocks exited'
    )

    lc1.init_logging.assert_called_once()
    lc2.init_logging.assert_not_called()


def test_nested_context_different_uuid() -> None:
    """This tests that two different configs are both initialized.

    The behaviour under test is that multiple configurations may
    "visit" a Python process and both be initialised, rather than
    one configuration being favoured.
    """
    lc1 = mock.Mock(LogConfig)
    lc1.uuid = str(uuid.uuid4())
    lc2 = mock.Mock(LogConfig)
    lc2.uuid = str(uuid.uuid4())

    lc1.init_logging.assert_not_called()
    lc2.init_logging.assert_not_called()
    with log_context(lc1):
        lc1.init_logging.assert_called_once()
        with log_context(lc2):
            lc2.init_logging.assert_called_once()
    lc1.init_logging.assert_called_once()
    lc2.init_logging.assert_called_once()


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
    with log_context(lc):
        logger = logging.getLogger()
        logger.info('Test logging')

        # TODO: assert the file exists and the string
        # "Test logging" appears in it.


def test_logging_with_jsonpool() -> None:
    # TODO: what sort of path override makes sense here? for users and
    # for testing? for testing, to keep test files out of  ~/.academy
    # _filepath = tmp_path / 'log.txt'
    lc = JSONPoolLogging()
    with log_context(lc):
        logger = logging.getLogger()
        logger.info('Test logging')

        path = pathlib.Path.home() / '.academy' / 'logs' / lc._pool_uuid

        files = list(path.iterdir())
        assert len(files) == 1, (
            'There should be one log file in the pool directory'
        )

        # TODO: assert the file exists and the string
        # "Test logging" appears in it.


def test_multi_config_repr() -> None:
    a = JSONPoolLogging()
    b = ConsoleLogging()

    lc = MultiLogging([a, b])

    assert repr(a) in repr(lc), (
        'MultiLogging repr should include subconfig repr'
    )
    assert repr(b) in repr(lc), (
        'MultiLogging repr should include subconfig repr'
    )


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
