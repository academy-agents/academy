from __future__ import annotations

import logging

from academy.logging import config
from academy.logging.configs.console import ConsoleLogging
from academy.logging.configs.file import FilePoolLog
from academy.logging.configs.multi import MultiLogConfig


def recommended_dev_log_config() -> config.ObservabilityConfig:
    """Returns a log configuration recommended for development use.

    This will configure console logging for academy WARNINGs and worse,
    and make debug-level logs for all Python logging into JSON formatted
    log files in ~/.academy/
    """
    return MultiLogConfig(
        [ConsoleLogging(level=logging.WARN), FilePoolLog()],
    )
