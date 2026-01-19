from __future__ import annotations

import logging
import pathlib

from academy.logging import config
from academy.logging.helpers import _Formatter
from academy.logging.helpers import _os_thread_filter

logger = logging.getLogger(__name__)


class FileLogging(config.ObservabilityConfig):
    """Configures logging to a file.

    This is the console part of academy.logging.init_logging.
    """

    def __init__(
        self,
        *,
        logfile: str | pathlib.Path,
        level: int | str = logging.INFO,
        extra: int = False,
    ) -> None:
        self.logfile = logfile
        self.level = level
        self.extra = extra

    def init_logging(self) -> Callable:
        """Initialize logging to file."""
        print('BENC: FileLogging initialization start')
        path = pathlib.Path(self.logfile)
        path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(path)
        file_handler.setFormatter(
            _Formatter(color=False, extra=self.extra),
        )
        file_handler.setLevel(self.level)
        if self.extra:
            file_handler.addFilter(_os_thread_filter)

        root_logger = logging.getLogger()
        root_logger.addHandler(file_handler)

        # if the root logger is not going to log at this level, reconfigure it to do so.
        # (smaller log levels = more logging)
        # for example, by default the root logger logs at level 30 WARNING, which
        # means INFO (20) logs will not be recorded. But implicitly if the user is asking
        # for INFO logs, we should provide INFO logs.
        root_logger.level = min(root_logger.level, file_handler.level)

        # This needs to be after the configuration of the root logger because
        # warnings get logged to a 'py.warnings' logger.
        # TODO: unclear about that ordering requirement?
        logging.captureWarnings(True)

        logger.info(
            'Configured logger (file-level=%s)',
            logging.getLevelName(self.level)
            if isinstance(self.level, int)
            else self.level,
        )
        file_handler.flush()
        print(f'BENC: FileLogging initialization end, {self.logfile}')
        print(
            f'BENC: FileLogging initialization end2, {self.logfile} root logger level={root_logger.level}',
        )

        # TODO: document: any live state (that shouldn't be serialized around to other users) should
        # be captured in a callable object to return here (e.g. a new object) rather than being stored
        # on this configuration object. live state is not part of the cross-process meaning of this
        # configuration.

        def uninitialize_callback():
            root_logger.removeHandler(file_handler)
            file_handler.close()

        return uninitialize_callback

    def __repr__(self):
        return (
            f'<file config {self.logfile}, {self.level}, extra={self.extra}>'
        )
