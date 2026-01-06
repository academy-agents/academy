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

    def init_logging(self) -> None:
        """Initialize logging to file."""
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

        # This needs to be after the configuration of the root logger because
        # warnings get logged to a 'py.warnings' logger.
        logging.captureWarnings(True)

        logger.info(
            'Configured logger (file-level=%s)',
            logging.getLevelName(self.level)
            if isinstance(self.level, int)
            else self.level,
        )
