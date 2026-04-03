from __future__ import annotations

import logging
import pathlib

import pytest
from pydantic import ValidationError

from academy.exchange.cloud.backend import PythonBackend
from academy.exchange.cloud.backend import RedisBackend
from academy.exchange.cloud.config import ExchangeAuthConfig
from academy.exchange.cloud.config import ExchangeServingConfig
from academy.exchange.cloud.config import LogConfig
from academy.exchange.cloud.config import PythonBackendConfig
from academy.exchange.cloud.config import RedisBackendConfig


# Like the logging tests, these are for coverage to make
# sure the code is functional.
def test_log_config_default() -> None:
    config = LogConfig()
    config.init_logger()

    logger = logging.getLogger()
    logger.info('Test logging')


def test_log_config_file(tmp_path: pathlib.Path) -> None:
    filepath = tmp_path / 'log.txt'
    config = LogConfig(logfile=str(filepath))
    config.init_logger()

    logger = logging.getLogger()
    logger.info('Test logging')


def test_log_config_file_rotation(tmp_path: pathlib.Path) -> None:
    filepath = tmp_path / 'log.txt'
    config = LogConfig(
        logfile=str(filepath),
        rotate=True,
    )
    config.init_logger()

    logger = logging.getLogger()
    logger.info('Test logging')


def test_auth_config_default() -> None:
    config = ExchangeAuthConfig()
    assert config.method is None


def test_python_backend_config() -> None:
    config = PythonBackendConfig()
    assert isinstance(config.get_backend(), PythonBackend)


def test_redis_backend_config_default() -> None:
    config = RedisBackendConfig()
    assert isinstance(config.get_backend(), RedisBackend)


def test_redis_backend_config_message_size() -> None:
    with pytest.raises(ValidationError):
        RedisBackendConfig(
            message_size_limit_kb=513 * 1024,
        )


def test_read_from_config_file_empty(tmp_path: pathlib.Path) -> None:
    data = ''

    filepath = tmp_path / 'relay.toml'
    with open(filepath, 'w') as f:
        f.write(data)

    config = ExchangeServingConfig.from_toml(filepath)
    assert config == ExchangeServingConfig()


def test_read_from_config_file(tmp_path: pathlib.Path) -> None:
    data = """\
host = "localhost"
port = 1234
certfile = "/path/to/cert.pem"
keyfile = "/path/to/privkey.pem"

[auth]
method = "globus"

[auth.kwargs]
client_id = "ABC"

[backend]
kind = "redis"
hostname = "localhost"
port = 1234
"""

    filepath = tmp_path / 'relay.toml'
    with open(filepath, 'w') as f:
        f.write(data)

    config = ExchangeServingConfig.from_toml(filepath)

    assert config.host == 'localhost'
    assert config.port == 1234  # noqa: PLR2004
    assert config.certfile == '/path/to/cert.pem'
    assert config.keyfile == '/path/to/privkey.pem'

    assert config.auth.method == 'globus'
    assert config.auth.kwargs['client_id'] == 'ABC'
    assert 'client_secret' not in config.auth.kwargs

    assert isinstance(config.backend, RedisBackendConfig)
    assert config.backend.port == 1234  # noqa: PLR2004
