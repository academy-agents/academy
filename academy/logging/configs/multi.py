from __future__ import annotations

import logging
from collections.abc import Callable

from academy.logging.configs.base import LogConfig

logger = logging.getLogger(__name__)


class MultiLogging(LogConfig):
    """This captures a collection of other LogConfigs.

    The configurations can be (de)initialized and moved around a distributed
    system all together.
    """

    def __init__(
        self,
        configs: list[LogConfig],
    ) -> None:
        super().__init__()
        self._configs = configs

    def init_logging(self) -> Callable[[], None]:
        """Initializes logging for all of the supplied configs."""
        uninits = [c.init_logging() for c in self._configs]

        def uninit_callback() -> None:
            for uninit in uninits:
                assert callable(uninit)
                uninit()

        return uninit_callback

    def __repr__(self) -> str:
        return (
            '<MultiLogging ['
            + ', '.join([repr(r) for r in self._configs])
            + '>'
        )
