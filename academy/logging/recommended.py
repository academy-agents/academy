from __future__ import annotations

import logging
import pathlib

from academy.logging.configs.base import LogConfig
from academy.logging.configs.console import ConsoleLogging
from academy.logging.configs.file import FileLogging
from academy.logging.configs.multi import MultiLogging


def recommended_logging(
    level: int | str = logging.INFO,
    *,
    logfile: str | pathlib.Path | None = None,
    logfile_level: int | str | None = None,
    color: bool = True,
    extra: int = False,
) -> LogConfig:
    """Returns a LogConfig that is hopefully useful to most users.

    The config object can be passed to other environments to get the same
    log configuration in those other environments.
    """
    configs: list[LogConfig] = []

    # Always makes a console logger.
    configs.append(ConsoleLogging(level=level, color=color, extra=extra))

    # If a log file is specified, make human readable logger
    if logfile is not None:
        logfile_level = level if logfile_level is None else logfile_level
        configs.append(
            FileLogging(logfile=logfile, level=logfile_level, extra=extra),
        )

    lc = configs[0] if len(configs) == 1 else MultiLogging(configs)

    return lc
