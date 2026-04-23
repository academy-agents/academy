from __future__ import annotations

import argparse
import asyncio
import importlib
import logging
import pathlib
import pickle
import sys
import uuid
from collections.abc import Sequence
from typing import Any
from typing import Generic
from typing import Literal
from typing import TYPE_CHECKING

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

import tomli_w
import tomllib
from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import model_validator

from academy.agent import Agent
from academy.debug import set_academy_debug
from academy.exchange import ExchangeFactory
from academy.exchange import GlobusExchangeFactory
from academy.exchange import HttpExchangeFactory
from academy.exchange import HybridExchangeFactory
from academy.exchange import RedisExchangeFactory
from academy.exchange.cloud.client import DEFAULT_EXCHANGE_URL
from academy.exchange.cloud.client import HttpAgentRegistration
from academy.exchange.cloud.globus import GlobusAgentRegistration
from academy.exchange.hybrid import HybridAgentRegistration
from academy.exchange.redis import RedisAgentRegistration
from academy.exchange.transport import AgentRegistration
from academy.logging.helpers import log_context
from academy.logging.recommended import recommended_logging
from academy.runtime import Runtime
from academy.runtime import RuntimeConfig

if TYPE_CHECKING:
    from academy.agent import AgentT
else:
    from academy.identifier import AgentT

logger = logging.getLogger(__name__)


class HttpExchangeModel(BaseModel):
    """Model for HttpExchangeFactory argument validation."""

    model_config = ConfigDict(extra='forbid')

    exchange_type: Literal['http'] = Field('http')
    url: str = Field(DEFAULT_EXCHANGE_URL)
    auth_method: Literal['globus'] | None = Field(None)
    additional_headers: dict[str, str] | None = Field(None)
    request_timeout_s: float = Field(60)
    ssl_verify: bool | None = None


class HybridExchangeModel(BaseModel):
    """Model for HybridExchangeFactory argument validation."""

    model_config = ConfigDict(extra='forbid')

    exchange_type: Literal['hybrid'] = Field('hybrid')
    redis_host: str
    redis_port: int
    redis_kwargs: dict[str, str] = Field(default_factory=dict)
    interface: str | None = Field(None)
    namespace: str = Field('default')
    ports: list[int] | None = Field(None)


class RedisExchangeModel(BaseModel):
    """Model for RedisExchangeFactory argument validation."""

    model_config = ConfigDict(extra='allow')  # Allow for reids kwargs

    exchange_type: Literal['redis'] = Field('redis')
    hostname: str
    port: int


class GlobusExchangeModel(BaseModel):
    """Model for GlobusExchangeFactory argument validation."""

    model_config = ConfigDict(extra='forbid')

    exchange_type: Literal['globus'] = Field('globus')
    project_id: uuid.UUID
    client_params: dict[str, Any] = Field(default_factory=dict)
    request_timeout_s: float = Field(60)


class AgentModel(BaseModel):
    """Model to construct agent from yaml or pickle."""

    model_config = ConfigDict(extra='allow')
    constructor: str | None = Field(None)
    pickle: pathlib.Path | None = Field(None)

    @model_validator(mode='after')
    def check_constructor_or_pickle(self) -> Self:
        """Ensures either the constructor or pickle file is non null."""
        if self.constructor is None and self.pickle is None:
            raise ValueError(
                'Either agent constructor or pickle file must be provided',
            )
        return self


class AgentProcessConfig(BaseModel, Generic[AgentT]):
    """Config for running an agent as an independent process."""

    model_config = ConfigDict(extra='forbid')
    exchange: (
        HttpExchangeModel
        | HybridExchangeModel
        | RedisExchangeModel
        | GlobusExchangeModel
    ) = Field(discriminator='exchange_type')
    agent_registration: (
        HttpAgentRegistration[AgentT]
        | HybridAgentRegistration[AgentT]
        | RedisAgentRegistration[AgentT]
        | GlobusAgentRegistration[AgentT]
        | None
    ) = Field(None, validate_default=False, discriminator='exchange_type')
    agent: AgentModel
    config: RuntimeConfig = Field(
        # By default, do not delete the mailbox on error or success
        default_factory=lambda: RuntimeConfig(
            terminate_on_error=False,
            terminate_on_success=False,
        ),
    )

    def get_exchange(self) -> ExchangeFactory[Any]:
        """Get the exchange factory specified by the config."""
        if self.exchange.exchange_type == 'http':
            return HttpExchangeFactory(
                **self.exchange.model_dump(exclude={'exchange_type'}),
            )
        elif self.exchange.exchange_type == 'hybrid':
            return HybridExchangeFactory(
                **self.exchange.model_dump(exclude={'exchange_type'}),
            )
        elif self.exchange.exchange_type == 'redis':
            return RedisExchangeFactory(
                **self.exchange.model_dump(exclude={'exchange_type'}),
            )
        elif self.exchange.exchange_type == 'globus':
            return GlobusExchangeFactory(
                **self.exchange.model_dump(exclude={'exchange_type'}),
            )

        raise AssertionError('Unreachable')

    def get_agent(self) -> AgentT:
        """Get the agent specified by the config."""
        agent: AgentT
        if self.agent.pickle:
            with open(self.agent.pickle, 'rb') as fp:
                agent = pickle.load(fp)
            if not isinstance(agent, Agent):
                raise TypeError(
                    'Pickled class is not of type Agent.',
                )
            return agent
        elif self.agent.constructor:
            module_path, _, name = self.agent.constructor.rpartition('.')
            if len(module_path) == 0:
                raise ValueError(
                    'Agent constructor must be the fully qualified path of '
                    f'an agent constructor. Got "{self.agent.constructor}".',
                )
            module = importlib.import_module(module_path)
            agent_const = getattr(module, name)
            agent = agent_const(**self.agent.model_extra)  # type: ignore[arg-type]
            if not isinstance(agent, Agent):
                raise TypeError(
                    'Agent constructor did not return an Agent type.',
                )
            return agent

        raise AssertionError('Unreachable')

    def to_toml(self, filepath: str | pathlib.Path) -> None:
        """Write the config to a toml file."""
        with open(filepath, 'wb') as f:
            tomli_w.dump(self.model_dump(exclude_none=True, mode='json'), f)

    @classmethod
    def load(cls, filepath: str | pathlib.Path) -> Self:
        """Parse an TOML config file."""
        with open(filepath, 'rb') as f:
            data = tomllib.load(f)
        return cls.model_validate(data)


async def _runtime_from_config(
    config: AgentProcessConfig[Any],
    config_file: str | pathlib.Path,
) -> Runtime[Any]:
    """Create a runtime from a config.

    Updates the config file with the agent registration if needed.

    Args:
        config: The configuration of the agent and runtime
        config_file: File to write updates to the config to.
    """
    exchange_factory = config.get_exchange()
    agent = config.get_agent()

    agent_registration: AgentRegistration[Any]
    if config.agent_registration is None:
        logger.info(
            'No registration in config, creating new mailbox for agent.',
        )
        async with await exchange_factory.create_user_client() as client:
            # Create a new mailbox for this agent
            agent_registration = await client.register_agent(type(agent))
            if not (
                config.config.terminate_on_error
                and config.config.terminate_on_success
            ):
                # If the mailbox is supposed to outlive the agent,
                # write the registration to file for future start up
                logger.info(
                    'Writing registration to config file for restarts.',
                    extra={'academy.agent_id': agent_registration.agent_id},
                )

                # mypy can't deduce these are the same type
                config.agent_registration = agent_registration  # type: ignore[assignment]
                config.to_toml(config_file)
    else:
        agent_registration = config.agent_registration  # type: ignore[assignment]

    return Runtime(
        agent,
        config=config.config,
        exchange_factory=exchange_factory,
        registration=agent_registration,
    )


async def _run_config(
    config: AgentProcessConfig[Any],
    config_file: str | pathlib.Path,
) -> int:
    """Build a runtime from a config, then run it.

    Args:
        config: The configuration of the agent and runtime
        config_file: File to write updates to the config to.
    """
    runtime = await _runtime_from_config(config, config_file)
    await runtime.run_until_complete()
    return 0


def _main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Use Academy debug mode',
    )
    parser.add_argument(
        '--config',
        help='Path to Academy RunConfig toml file.',
        required=True,
    )
    parser.add_argument(
        '--log-level',
        help='Verbosity of logging default=INFO.',
        default=logging.INFO,
    )
    parser.add_argument(
        '--log-file',
        help='Path to write logs.',
    )
    argv = sys.argv[1:] if argv is None else argv
    args = parser.parse_args(argv)
    config: AgentProcessConfig[Any] = AgentProcessConfig.load(args.config)

    # TODO: This is temporary until we decide how to implement configurable
    # logging with a yaml file
    log_config = recommended_logging(args.log_level, logfile=args.log_file)
    with log_context(log_config):
        set_academy_debug(args.debug)
        return asyncio.run(_run_config(config, args.config))


if __name__ == '__main__':
    raise SystemExit(_main())
