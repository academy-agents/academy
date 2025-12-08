from __future__ import annotations

import logging
import pathlib
import sys
import uuid

logger = logging.getLogger(__name__)

from academy.logging import _Formatter
from academy.logging import _os_thread_filter
from academy.logging import JSONHandler


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


class FilePoolLog(ObservabilityConfig):
    """Configures logging to files in home directory log pool.

    This is intended to make it so json logs go into a uniquely identified
    filename, identified first by the instance of the file pool log, then
    by a configuration invocation ID, so that we end up with a pool of logs
    that are clearly related. Even if they are on different file systems,
    in the distributed case, they should at least all appear on those file
    systems under the same pool instance ID.

    The logs are not configurable: they are full debug logs from the root,
    in JSON format. That's intended to get *rid* of configurability for users
    so that they keep the full observability level of logging.
    """

    def __init__(
        self,
    ):
        self._pool_uuid = str(uuid.uuid4())

    def init_logging(self):
        # in the parsl prototype, this contains some more context such as
        # a supplied component name. there is also the opportunity for
        # fancy stack inspection to # see who invoked init_logging to
        # fabricate more human readable names, but a core json log notion is
        # that you wouldn't be looking at individual files manually anyway.
        instance_id = str(uuid.uuid4())

        # home resolution is deferred until init_logging time because can
        # be different on every invocation as the config object is moved
        # between execution locations.
        path = (
            pathlib.Path.home()
            / '.academy'
            / 'logs'
            / self._pool_uuid
            / instance_id
        )
        path.parent.mkdir(parents=True, exist_ok=True)

        json_handler = JSONHandler(path.with_suffix('.jsonlog'))
        json_handler.setLevel(logging.DEBUG)

        root_logger = logging.getLogger()
        root_logger.addHandler(json_handler)
        root_logger.setLevel(logging.DEBUG)  # this is global ugh

        logger.info(
            'Configured FilePoolLoggger logger (pool uuid=%s, path=%s)',
            self._pool_uuid,
            path,
        )
