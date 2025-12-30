from __future__ import annotations

import logging

import academy.observability.examples as loggers
from academy.logging import config


def recommended_dev_log_config() -> config.ObservabilityConfig:
    """Returns a log configuration recommended for development use.

    This will configure console logging for academy WARNINGs and worse,
    and make debug-level logs for all Python logging into JSON formatted
    log files in ~/.academy/
    """
    return loggers.MultiLogConfig(
        [loggers.ConsoleLogging(level=logging.WARN), loggers.FilePoolLog()],
    )
