from __future__ import annotations

import logging
import pathlib
import uuid
from collections.abc import Callable

from academy.logging import config
from academy.logging.helpers import JSONHandler

logger = logging.getLogger(__name__)


class FilePoolLog(config.ObservabilityConfig):
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
    ) -> None:
        self._pool_uuid = str(uuid.uuid4())

    def init_logging(self) -> Callable[[], None]:
        """Initialize JSON logging into shared pool."""
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
        root_logger.setLevel(
            logging.DEBUG,
        )  # TODO: this is global ugh but see the `min`
        # approach I did in file logger

        logger.info(
            'Configured FilePoolLoggger logger (pool uuid=%s, path=%s)',
            self._pool_uuid,
            path,
        )

        def uninitialize_callback() -> None:
            root_logger.removeHandler(json_handler)
            json_handler.close()

        return uninitialize_callback
