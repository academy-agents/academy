from __future__ import annotations

import logging
import pathlib
import uuid
from collections.abc import Callable

from academy.logging.configs.base import LogConfig
from academy.logging.helpers import JSONHandler

logger = logging.getLogger(__name__)


class JSONPoolLogging(LogConfig):
    """Configures logging to files in home directory based pool of logs.

    This feature is aimed at mechanical processing of logs, rather than
    human readability.

    Logs written by this configuration are stored in JSON format under
    `~/local/share/academy/logs/`

    Under that directory, logs are first separated into directories by
    the (distributed) identity of the log configuration: logs configured
    by the same JSONPoolLogging object, or by a serialized/deserialized
    copy of the same object, will appear under the same directory.

    Within that directory, each instance of log initialization will get a
    new log file.

    This logger is not configurable: it is intended to capture full debug
    logs from the root, with selection of log records made during the
    mechanical analysis stage.
    """

    def __init__(
        self,
    ) -> None:
        super().__init__()

    def init_logging(self) -> Callable[[], None]:
        """Initialize JSON logging into shared pool."""
        instance_id = str(uuid.uuid4())

        # Home resolution is deferred until init_logging time because the path
        # can be different on every invocation as the config object is moved
        # between execution hosts.
        path = (
            pathlib.Path.home()
            / 'local'
            / 'share'
            / 'academy'
            / 'logs'
            / self.uuid
            / instance_id
        )
        path.parent.mkdir(parents=True, exist_ok=True)

        json_handler = JSONHandler(path.with_suffix('.jsonlog'))
        json_handler.setLevel(logging.DEBUG)

        root_logger = logging.getLogger()
        root_logger.addHandler(json_handler)
        root_logger.level = min(root_logger.level, json_handler.level)

        logger.info(
            'Configured JSONPoolLogging (pool uuid=%s, path=%s)',
            self.uuid,
            path,
        )

        def uninitialize_callback() -> None:
            root_logger.removeHandler(json_handler)
            json_handler.close()

        return uninitialize_callback
