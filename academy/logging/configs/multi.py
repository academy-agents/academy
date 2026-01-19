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
        uninits = [c.init_logging for c in self._configs]

        def uninit_callback():
            for uninit in uninits:
                assert callable(uninit)
                uninit()

        return uninit_callback
