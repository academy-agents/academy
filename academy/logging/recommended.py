from __future__ import annotations

import logging
import pathlib

from academy.logging import config
from academy.logging.configs.console import ConsoleLogging
from academy.logging.configs.file import FileLogging
from academy.logging.configs.jsonpool import FilePoolLog
from academy.logging.configs.multi import MultiLogConfig

# TODO: here's an example new configuration...


def recommended_dev_log_config() -> config.ObservabilityConfig:
    """Returns a log configuration recommended for development use.

    This will configure console logging for academy WARNINGs and worse,
    and make debug-level logs for all Python logging into JSON formatted
    log files in ~/.academy/
    """
    return MultiLogConfig(
        [ConsoleLogging(level=logging.WARN), FilePoolLog()],
    )


# TODO: ... and here's my attempt to replicate the old configuration,
# with some extras


def init_logging(  # noqa: PLR0913
    level: int | str = logging.INFO,
    *,
    logfile: str | pathlib.Path | None = None,
    logfile_level: int | str | None = None,
    color: bool = True,
    extra: int = False,
    force: bool = False,
    # TODO: what to do about force? its horrible and it broke
    # something before in tests...
) -> config.ObservabilityConfig:
    """Initialize process global logger and return config for further use.

    The config object can be passed to other environments to get the same
    log configuration in those other environments.
    """
    configs: list[config.ObservabilityConfig] = []

    # Always makes a console logger.
    configs.append(ConsoleLogging(level=level, color=color, extra=extra))

    # If a log file is specified, make human readable logger
    if logfile is not None:
        logfile_level = level if logfile_level is None else logfile_level
        configs.append(
            FileLogging(logfile=logfile, level=logfile_level, extra=extra),
        )

    # TODO: json handler - maybe ignore the old level 2 json handling
    # and implement this differently? to put things in the log pool in ~
    # rather than in an ad-hoc specified non-portable directory?
    # which would work orthogonally to specifying the logfile path
    # explicitly.

    lc = configs[0] if len(configs) == 1 else MultiLogConfig(configs)

    lc.init_logging()
    return lc
