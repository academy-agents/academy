from __future__ import annotations

import json
import logging
import pathlib
import threading

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
                f'{self.green}[tid=%(os_thread)d pid=%(process)d '
                f'task=%(taskName)s] {self.reset} '
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
