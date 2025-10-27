from __future__ import annotations

import logging
import pathlib
import sys
import threading


class _Formatter(logging.Formatter):
    def __init__(self, color: bool = False, extra: bool = False) -> None:
        if color:
            grey = '\033[2;37m'
            green = '\033[32m'
            cyan = '\033[36m'
            blue = '\033[34m'
            yellow = '\033[33m'
            red = '\033[31m'
            purple = '\033[35m'
            reset = '\033[0m'
        else:
            grey = ''
            green = ''
            cyan = ''
            blue = ''
            yellow = ''
            red = ''
            purple = ''
            reset = ''

        if extra:
            extra_fmt = f'{green}[tid=%(os_thread)d pid=%(process)d]{reset} '
        else:
            extra_fmt = ''

        datefmt = '%Y-%m-%d %H:%M:%S'
        logfmt = (
            f'{grey}[%(asctime)s.%(msecs)03d]{reset} {extra_fmt}'
            f'{{level}}%(levelname)-8s{reset} '
            f'{purple}(%(name)s){reset} %(message)s'
        )
        debug_fmt = logfmt.format(level=cyan)
        info_fmt = logfmt.format(level=blue)
        warning_fmt = logfmt.format(level=yellow)
        error_fmt = logfmt.format(level=red)

        self.formatters = {
            logging.DEBUG: logging.Formatter(debug_fmt, datefmt=datefmt),
            logging.INFO: logging.Formatter(info_fmt, datefmt=datefmt),
            logging.WARNING: logging.Formatter(warning_fmt, datefmt=datefmt),
            logging.ERROR: logging.Formatter(error_fmt, datefmt=datefmt),
            logging.CRITICAL: logging.Formatter(error_fmt, datefmt=datefmt),
        }

    def format(self, record: logging.LogRecord) -> str:  # pragma: no cover
        extras = ''
        for k, v in record.__dict__.items():
            if k.startswith('academy.'):
                extras += f' {k}={v!r}'

        grey = '\033[2;37m'
        if extras != '':
            extras = '\n' + grey + extras

        return self.formatters[record.levelno].format(record) + extras


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
    extra: bool = False,
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
            process ID. This is helpful for debugging.
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
        handler = logging.FileHandler(path)
        handler.setFormatter(_Formatter(color=False, extra=extra))
        handler.setLevel(logfile_level)
        handlers.append(handler)

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
