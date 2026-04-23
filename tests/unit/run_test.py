from __future__ import annotations

import pickle
import uuid
from pathlib import Path
from typing import Any
from unittest import mock

import pytest

from academy.exchange import ExchangeFactory
from academy.exchange.redis import RedisExchangeFactory
from academy.run import _main
from academy.run import _runtime_from_config
from academy.run import AgentModel
from academy.run import AgentProcessConfig
from academy.run import HttpExchangeModel
from academy.run import RedisExchangeModel
from academy.runtime import Runtime
from academy.runtime import RuntimeConfig
from testing.agents import SleepAgent

CONFIGS = (
    {
        'exchange': {
            'exchange_type': 'http',
            'url': 'http://127.0.0.1',
        },
        'agent_registration': {
            'exchange_type': 'http',
            'agent_id': {'uid': str(uuid.uuid4())},
        },
        'agent': {
            'constructor': 'testing.agents.SleepAgent',
        },
    },
    {
        'exchange': {
            'exchange_type': 'redis',
            'hostname': '127.0.0.1',
            'port': '6789',
        },
        'agent_registration': {
            'exchange_type': 'redis',
            'agent_id': {'uid': str(uuid.uuid4())},
        },
        'agent': {
            'constructor': 'testing.agents.SleepAgent',
        },
    },
    {
        'exchange': {
            'exchange_type': 'hybrid',
            'redis_host': '127.0.0.1',
            'redis_port': '6789',
        },
        'agent_registration': {
            'exchange_type': 'hybrid',
            'agent_id': {'uid': str(uuid.uuid4())},
        },
        'agent': {
            'constructor': 'testing.agents.SleepAgent',
        },
    },
    {
        'exchange': {
            'exchange_type': 'globus',
            'project_id': uuid.uuid4(),
        },
        'agent_registration': {
            'exchange_type': 'globus',
            'agent_id': {'uid': str(uuid.uuid4())},
            'client_id': uuid.uuid4(),
            'token': 'token',
            'secret': 'secret',
        },
        'agent': {
            'constructor': 'testing.agents.SleepAgent',
        },
    },
)


@pytest.mark.parametrize(
    'config_dict',
    CONFIGS,
)
def test_config_get_exchange(
    config_dict: dict[str, Any],
):
    config = AgentProcessConfig.model_validate(config_dict)
    exchange_factory = config.get_exchange()
    assert isinstance(exchange_factory, ExchangeFactory)


def test_agent_model_no_constructor_or_pickle():
    with pytest.raises(
        ValueError,
        match='Either agent constructor or pickle file must be provided',
    ):
        AgentModel.model_validate({})


def test_config_get_agent():
    config = AgentProcessConfig(
        exchange=HttpExchangeModel(),
        agent=AgentModel(
            constructor='testing.agents.SleepAgent',
        ),
    )
    agent = config.get_agent()
    assert isinstance(agent, SleepAgent)


def test_config_get_agent_with_params():
    config = AgentProcessConfig(
        exchange=HttpExchangeModel(),
        agent=AgentModel(
            constructor='testing.agents.SleepAgent',
            loop_sleep=1,
        ),
    )
    agent = config.get_agent()
    assert isinstance(agent, SleepAgent)
    assert agent.loop_sleep == 1


def test_config_get_agent_bad_path():
    config = AgentProcessConfig(
        exchange=HttpExchangeModel(),
        agent=AgentModel(
            constructor='SleepAgent',
        ),
    )
    with pytest.raises(
        ValueError,
        match='fully qualified path',
    ):
        config.get_agent()


def test_config_get_agent_incorrect_type():
    config = AgentProcessConfig(
        exchange=HttpExchangeModel(),
        agent=AgentModel(
            constructor='time.time',
        ),
    )
    with pytest.raises(TypeError):
        config.get_agent()


def test_config_get_agent_pickle(tmp_path: Path):
    agent_file = tmp_path / 'agent.pkl'
    agent = SleepAgent()
    with open(agent_file, 'wb') as fp:
        pickle.dump(agent, fp)

    config = AgentProcessConfig(
        exchange=HttpExchangeModel(),
        agent=AgentModel(
            pickle=agent_file,
        ),
    )
    loaded_agent = config.get_agent()
    assert isinstance(loaded_agent, SleepAgent)


class NotAnAgent: ...


def test_config_get_agent_pickle_incorrect_type(tmp_path: Path):
    agent_file = tmp_path / 'agent.pkl'
    not_an_agent = NotAnAgent()
    with open(agent_file, 'wb') as fp:
        pickle.dump(not_an_agent, fp)

    config = AgentProcessConfig(
        exchange=HttpExchangeModel(),
        agent=AgentModel(
            pickle=agent_file,
        ),
    )
    with pytest.raises(TypeError):
        config.get_agent()


@pytest.fixture
def config_file(tmp_path) -> Path:
    config_toml = """
[exchange]
exchange_type = 'redis'
hostname = '127.0.0.1'
port = '6789'

[agent]
constructor = 'testing.agents.SleepAgent'
loop_sleep = 1
"""
    config_file = tmp_path / 'config.toml'
    with open(config_file, 'w') as fp:
        fp.write(config_toml)

    return config_file


def test_read_config_from_toml(config_file: Path):
    config = AgentProcessConfig.load(config_file)
    exchange = config.get_exchange()
    assert isinstance(exchange, RedisExchangeFactory)

    agent = config.get_agent()
    assert isinstance(agent, SleepAgent)
    assert agent.loop_sleep == 1


def test_write_config_to_toml(tmp_path):
    config = AgentProcessConfig(
        exchange=RedisExchangeModel(
            hostname='127.0.0.1',
            port='6789',
        ),
        agent=AgentModel(
            constructor='testing.agents.SleepAgent',
        ),
    )

    config_file = tmp_path / 'config.toml'
    config.to_toml(config_file)

    config = AgentProcessConfig.load(config_file)
    exchange = config.get_exchange()
    assert isinstance(exchange, RedisExchangeFactory)

    agent = config.get_agent()
    assert isinstance(agent, SleepAgent)


@pytest.mark.asyncio
async def test_runtime_from_config_writes_registration_to_file(
    config_file: Path,
    mock_redis,
):
    config = AgentProcessConfig.load(config_file)
    assert config.agent_registration is None
    runtime = await _runtime_from_config(config, config_file)

    new_config = AgentProcessConfig.load(config_file)
    assert new_config.agent_registration is not None
    assert new_config.agent_registration == runtime.registration


@pytest.mark.asyncio
async def test_runtime_from_config_existing_registration(
    config_file: Path,
    mock_redis,
):
    config = AgentProcessConfig.load(config_file)
    assert config.agent_registration is None

    await _runtime_from_config(config, config_file)
    new_config = AgentProcessConfig.load(config_file)
    assert new_config.agent_registration is not None

    runtime = await _runtime_from_config(config, config_file)
    assert runtime.registration == new_config.agent_registration


@pytest.mark.asyncio
async def test_run_temporary_registration(config_file: Path, mock_redis):
    config = AgentProcessConfig.load(config_file)
    config.config = RuntimeConfig(
        terminate_on_error=True,
        terminate_on_success=True,
    )
    assert config.agent_registration is None

    await _runtime_from_config(config, config_file)
    new_config = AgentProcessConfig.load(config_file)
    assert new_config.agent_registration is None


def test_main(tmp_path: Path, config_file: Path, mock_redis):
    log_file = tmp_path / 'run.log'
    with mock.patch.object(Runtime, 'run_until_complete'):
        rc = _main(
            [
                '--config',
                str(config_file),
                '--log-level',
                'DEBUG',
                '--log-file',
                str(log_file),
            ],
        )
        assert rc == 0
