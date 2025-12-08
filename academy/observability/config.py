from __future__ import annotations

import logging
import sys

logger = logging.getLogger(__name__)

from academy.logging import _Formatter
from academy.logging import _os_thread_filter


class ObservabilityConfig:
    """Implementations of this class can initialize observability.

    That means they know how to initialize and de-initialize
    structured logging in the process they are running in,
    in whatever observability context makes sense for the
    object.

    Objects should expect to be pickled/unpickled multiple
    times as they move around an academy distributed system.
    That leads to knock-on requirements like implementing
    classes being importable in all relevant locations, and
    being careful about what state is stored when initializing
    logging.
    """

    def init_logging(self):
        """Hosting environments will call this on the object
        they have been configured with to initialize
        observability.

        It is up for discussion where the relevant points to
        call this are: for example, academy might want to
        initialize logging around multiple agents which all
        exist within a broader htex worker (e.g. when running
        inside Globus Compute).

        This should return a callback that will uninitialize
        logging, which can be called by the same hosting
        environments.
        """
        pass


class ConsoleLogging(ObservabilityConfig):
    """Configures logging to the console. This is the
    console part of academy.logging.init_logging
    """

    def __init__(
        self,
        *,
        level: int | str = logging.INFO,
        color: bool = True,
        extra: int = False,
    ):
        self.level = level
        self.color = color
        self.extra = extra

    def init_logging(self):
        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setFormatter(
            _Formatter(color=self.color, extra=self.extra),
        )
        stdout_handler.setLevel(self.level)
        if self.extra:
            stdout_handler.addFilter(_os_thread_filter)

        rootLogger = logging.getLogger()
        rootLogger.addHandler(stdout_handler)

        # This needs to be after the configuration of the root logger because
        # warnings get logged to a 'py.warnings' logger.
        logging.captureWarnings(True)

        logger.info(
            'Configured logger (stdout-level=%s)',
            logging.getLevelName(self.level)
            if isinstance(self.level, int)
            else self.level,
        )
