from __future__ import annotations

import logging
import sys
from collections.abc import Callable

from academy.logging import config
from academy.logging.helpers import _Formatter
from academy.logging.helpers import _os_thread_filter

logger = logging.getLogger(__name__)


class ConsoleLogging(config.ObservabilityConfig):
    """Configures logging to the console.

    TODO: This is the console part of academy.logging.init_logging.
    """

    def __init__(
        self,
        *,
        level: int | str = logging.INFO,
        color: bool = True,
        extra: int = False,
    ) -> None:
        super().__init__()
        self.level = level
        self.color = color
        self.extra = extra

    def init_logging(self) -> Callable[[], None]:
        """Initialize logging to console."""
        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setFormatter(
            _Formatter(color=self.color, extra=self.extra),
        )
        stdout_handler.setLevel(self.level)
        if self.extra:
            stdout_handler.addFilter(_os_thread_filter)

        root_logger = logging.getLogger()
        root_logger.addHandler(stdout_handler)
        root_logger.level = min(root_logger.level, stdout_handler.level)

        # This needs to be after the configuration of the root logger because
        # warnings get logged to a 'py.warnings' logger.
        logging.captureWarnings(True)

        logger.info(
            'Configured logger (stdout-level=%s)',
            logging.getLevelName(self.level)
            if isinstance(self.level, int)
            else self.level,
        )

        def uninitialize_callback() -> None:
            root_logger.removeHandler(stdout_handler)
            stdout_handler.close()

        return uninitialize_callback
