from __future__ import annotations

import logging

from academy.logging import config

logger = logging.getLogger(__name__)


class MultiLogConfig(config.ObservabilityConfig):
    """This captures a collection of other observability configs."""

    def __init__(self, configs: list[config.ObservabilityConfig]) -> None:
        self._configs = configs

    def init_logging(self) -> None:
        """Initializes logging for all of the supplied configs."""
        for c in self._configs:
            c.init_logging()
