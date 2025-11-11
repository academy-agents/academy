from __future__ import annotations

import json
import logging
import pathlib
import sys
import threading
from asyncio import Future
from typing import Any

logger = logging.getLogger(__name__)

# extra keys with this prefix will be added to human-readable logs
# when `extra > 1`
ACADEMY_EXTRA_PREFIX = 'academy.'


class _Formatter(logging.Formatter):
    def __init__(self, color: bool = False, extra: int = False) -> None:
        self.extra = extra
        if color:
            self.grey = '\033[2;37m'
            self.green = '\033[32m'
            self.cyan = '\033[36m'
            self.blue = '\033[34m'
            self.yellow = '\033[33m'
            self.red = '\033[31m'
            self.purple = '\033[35m'
            self.reset = '\033[0m'
        else:
            self.grey = ''
            self.green = ''
            self.cyan = ''
            self.blue = ''
            self.yellow = ''
            self.red = ''
            self.purple = ''
            self.reset = ''

        if extra:
            extra_fmt = (
                f'{self.green}[tid=%(os_thread)d pid=%(process)d]{self.reset} '
            )
        else:
            extra_fmt = ''

        datefmt = '%Y-%m-%d %H:%M:%S'
        logfmt = (
            f'{self.grey}[%(asctime)s.%(msecs)03d]{self.reset} {extra_fmt}'
            f'{{level}}%(levelname)-8s{self.reset} '
            f'{self.purple}(%(name)s){self.reset} %(message)s'
        )
        debug_fmt = logfmt.format(level=self.cyan)
        info_fmt = logfmt.format(level=self.blue)
        warning_fmt = logfmt.format(level=self.yellow)
        error_fmt = logfmt.format(level=self.red)

        self.formatters = {
            logging.DEBUG: logging.Formatter(debug_fmt, datefmt=datefmt),
            logging.INFO: logging.Formatter(info_fmt, datefmt=datefmt),
            logging.WARNING: logging.Formatter(warning_fmt, datefmt=datefmt),
            logging.ERROR: logging.Formatter(error_fmt, datefmt=datefmt),
            logging.CRITICAL: logging.Formatter(error_fmt, datefmt=datefmt),
        }

    def format(self, record: logging.LogRecord) -> str:  # pragma: no cover
        if self.extra > 1:
            kvs = [
                (k, v)
                for (k, v) in record.__dict__.items()
                if k.startswith(ACADEMY_EXTRA_PREFIX)
            ]
            if kvs:
                end_line = ''
                for k, v in kvs:
                    k_trimmed = k[len(ACADEMY_EXTRA_PREFIX) :]
                    end_line += f' {self.yellow}{k_trimmed}: {self.purple}{v}'
                end_line += self.reset
            else:
                end_line = ''
            return self.formatters[record.levelno].format(record) + end_line

        else:
            return self.formatters[record.levelno].format(record)


def _os_thread_filter(
    record: logging.LogRecord,
) -> logging.LogRecord:  # pragma: no cover
    record.os_thread = threading.get_native_id()
    return record


def init_logging(  # noqa: PLR0913
    level: int | str = logging.INFO,
    *,
    logfile: str | pathlib.Path | None = None,
    logfile_level: int | str | None = None,
    color: bool = True,
    extra: int = False,
    force: bool = False,
) -> logging.Logger:
    """Initialize global logger.

    Args:
        level: Minimum logging level.
        logfile: Configure a file handler for this path.
        logfile_level: Minimum logging level for the file handler. Defaults
            to that of `level`.
        color: Use colorful logging for stdout.
        extra: Include extra info in log messages, such as thread ID and
            process ID. This is helpful for debugging. True or 1 adds some
            extra info. 2 adds on observability-style logging of key-value
            metadata, and adds a second logfile formatted as JSON.
        force: Remove any existing handlers attached to the root
            handler. This option is useful to silencing the third-party
            package logging. Note: should not be set when running inside
            pytest.

    Returns:
        The root logger.
    """
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(_Formatter(color=color, extra=extra))
    stdout_handler.setLevel(level)
    if extra:
        stdout_handler.addFilter(_os_thread_filter)
    handlers: list[logging.Handler] = [stdout_handler]

    if logfile is not None:
        logfile_level = level if logfile_level is None else logfile_level
        path = pathlib.Path(logfile)
        path.parent.mkdir(parents=True, exist_ok=True)
        human_handler = logging.FileHandler(path)
        human_handler.setFormatter(_Formatter(color=False, extra=extra))
        human_handler.setLevel(logfile_level)
        if extra:
            human_handler.addFilter(_os_thread_filter)
        handlers.append(human_handler)

        if extra > 1:
            json_handler = JSONHandler(path.with_suffix('.jsonlog'))
            json_handler.setLevel(logfile_level)
            handlers.append(json_handler)

    logging.basicConfig(
        datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.NOTSET,
        handlers=handlers,
        force=force,
    )

    # This needs to be after the configuration of the root logger because
    # warnings get logged to a 'py.warnings' logger.
    logging.captureWarnings(True)

    logger = logging.getLogger()
    logger.info(
        'Configured logger (stdout-level=%s, logfile=%s, logfile-level=%s)',
        logging.getLevelName(level) if isinstance(level, int) else level,
        logfile,
        logging.getLevelName(logfile_level)
        if isinstance(logfile_level, int)
        else logfile_level,
    )

    return logger


class JSONHandler(logging.Handler):
    """A LogHandler which outputs records as JSON objects, one per line."""

    def __init__(self, filename: pathlib.Path) -> None:
        super().__init__()
        self.f = open(filename, 'w')  # noqa: SIM115

    def emit(self, record: logging.LogRecord) -> None:
        """Emits the log record as a JSON object.

        Each attribute (including extra attributes) of the log record becomes
        an entry in the JSON object. Each value is rendered using ``str``.
        """
        d = {}

        d['formatted'] = self.format(record)

        for k, v in record.__dict__.items():
            try:
                d[k] = str(v)
            except Exception as e:
                d[k] = f'Unrepresentable: {e!r}'

        json.dump(d, fp=self.f)
        print('', file=self.f)
        self.f.flush()


async def execute_and_log_traceback(
    fut: Future[Any],
) -> Any:
    """Await a future and log any exception..

    Catches any exceptions raised by the coroutine, logs the traceback,
    and re-raises the exception.
    """
    try:
        return await fut
    except Exception:
        logger.exception('Background task raised an exception.')
        raise
