# ruff: noqa: D102
from __future__ import annotations

import dataclasses
import enum
import logging
import sys
import uuid
from typing import Any
from typing import Generic
from typing import get_args
from typing import NamedTuple

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

import redis.asyncio

from academy.behavior import Behavior
from academy.behavior import BehaviorT
from academy.exception import BadEntityIdError
from academy.exception import MailboxTerminatedError
from academy.exchange import ExchangeFactory
from academy.exchange.transport import ExchangeTransportMixin
from academy.exchange.transport import MailboxStatus
from academy.identifier import AgentId
from academy.identifier import EntityId
from academy.identifier import UserId
from academy.message import BaseMessage
from academy.message import Message
from academy.serialize import NoPickleMixin

logger = logging.getLogger(__name__)

_CLOSE_SENTINEL = b'<CLOSED>'


class _RedisConnectionInfo(NamedTuple):
    hostname: str
    port: int
    kwargs: dict[str, Any]


class _MailboxState(enum.Enum):
    ACTIVE = 'ACTIVE'
    INACTIVE = 'INACTIVE'


@dataclasses.dataclass
class RedisAgentRegistration(Generic[BehaviorT]):
    """Agent registration for redis exchanges."""

    agent_id: AgentId[BehaviorT]
    """Unique identifier for the agent created by the exchange."""


class RedisExchangeTransport(ExchangeTransportMixin, NoPickleMixin):
    """Redis exchange transport bound to a specific mailbox."""

    def __init__(
        self,
        mailbox_id: EntityId,
        redis_client: redis.asyncio.Redis,
        *,
        redis_info: _RedisConnectionInfo,
    ) -> None:
        self._mailbox_id = mailbox_id
        self._client = redis_client
        self._redis_info = redis_info

    def _active_key(self, uid: EntityId) -> str:
        return f'active:{uid.uid}'

    def _behavior_key(self, uid: AgentId[Any]) -> str:
        return f'behavior:{uid.uid}'

    def _queue_key(self, uid: EntityId) -> str:
        return f'queue:{uid.uid}'

    @classmethod
    async def new(
        cls,
        *,
        mailbox_id: EntityId | None = None,
        name: str | None = None,
        redis_info: _RedisConnectionInfo,
    ) -> Self:
        """Instantiate a new transport.

        Args:
            mailbox_id: Bind the transport to the specific mailbox. If `None`,
                a new user entity will be registered and the transport will be
                bound to that mailbox.
            name: Display name of the redistered entity if `mailbox_id` is
                `None`.
            redis_info: Redis connection information.

        Returns:
            An instantiated transport bound to a specific mailbox.

        Raises:
            redis.exceptions.ConnectionError: If the Redis server is not
                reachable.
        """
        client = redis.asyncio.Redis(
            host=redis_info.hostname,
            port=redis_info.port,
            decode_responses=False,
            **redis_info.kwargs,
        )
        # Ensure the redis server is reachable else fail early
        await client.ping()

        if mailbox_id is None:
            mailbox_id = UserId.new(name=name)
            await client.set(
                f'active:{mailbox_id.uid}',
                _MailboxState.ACTIVE.value,
            )
            logger.info('Registered %s in exchange', mailbox_id)
        return cls(mailbox_id, client, redis_info=redis_info)

    @property
    def mailbox_id(self) -> EntityId:
        return self._mailbox_id

    async def close(self) -> None:
        await self._client.aclose()

    async def discover(
        self,
        behavior: type[Behavior],
        *,
        allow_subclasses: bool = True,
    ) -> tuple[AgentId[Any], ...]:
        found: list[AgentId[Any]] = []
        fqp = f'{behavior.__module__}.{behavior.__name__}'
        async for key in self._client.scan_iter(
            'behavior:*',
        ):  # pragma: no branch
            mro_str = await self._client.get(key)
            assert isinstance(mro_str, str)
            mro = mro_str.split(',')
            if fqp == mro[0] or (allow_subclasses and fqp in mro):
                aid: AgentId[Any] = AgentId(uid=uuid.UUID(key.split(':')[-1]))
                found.append(aid)
        active: list[AgentId[Any]] = []
        for aid in found:
            status = await self._client.get(self._active_key(aid))
            if status == _MailboxState.ACTIVE.value:  # pragma: no branch
                active.append(aid)
        return tuple(active)

    def factory(self) -> RedisExchangeFactory:
        return RedisExchangeFactory(
            hostname=self._redis_info.hostname,
            port=self._redis_info.port,
            **self._redis_info.kwargs,
        )

    async def recv(self, timeout: float | None = None) -> Message:
        _timeout = timeout if timeout is not None else 0
        status = await self._client.get(
            self._active_key(self.mailbox_id),
        )
        if status is None:
            raise AssertionError(
                f'Status for mailbox {self.mailbox_id} did not exist in '
                'Redis server. This means that something incorrectly '
                'deleted the key.',
            )
        elif status == _MailboxState.INACTIVE.value:
            raise MailboxTerminatedError(self.mailbox_id)

        raw = await self._client.blpop(  # type: ignore[misc]
            [self._queue_key(self.mailbox_id)],
            timeout=_timeout,
        )
        if raw is None:
            raise TimeoutError(
                f'Timeout waiting for next message for {self.mailbox_id} '
                f'after {timeout} seconds.',
            )

        # Only passed one key to blpop to result is [key, item]
        assert isinstance(raw, (tuple, list))
        assert len(raw) == 2  # noqa: PLR2004
        if raw[1] == _CLOSE_SENTINEL:  # pragma: no cover
            raise MailboxTerminatedError(self.mailbox_id)
        message = BaseMessage.model_deserialize(raw[1])
        assert isinstance(message, get_args(Message))
        return message

    async def register_agent(
        self,
        behavior: type[BehaviorT],
        *,
        name: str | None = None,
    ) -> RedisAgentRegistration[BehaviorT]:
        aid: AgentId[BehaviorT] = AgentId.new(name=name)
        await self._client.set(
            self._active_key(aid),
            _MailboxState.ACTIVE.value,
        )
        await self._client.set(
            self._behavior_key(aid),
            ','.join(behavior.behavior_mro()),
        )
        return RedisAgentRegistration(agent_id=aid)

    async def send(self, message: Message) -> None:
        status = await self._client.get(self._active_key(message.dest))
        if status is None:
            raise BadEntityIdError(message.dest)
        elif status == _MailboxState.INACTIVE.value:
            raise MailboxTerminatedError(message.dest)
        else:
            await self._client.rpush(  # type: ignore[misc]
                self._queue_key(message.dest),
                message.model_serialize(),
            )

    async def status(self, uid: EntityId) -> MailboxStatus:
        status = await self._client.get(self._active_key(uid))
        if status is None:
            return MailboxStatus.MISSING
        elif status == _MailboxState.INACTIVE.value:
            return MailboxStatus.TERMINATED
        else:
            return MailboxStatus.ACTIVE

    async def terminate(self, uid: EntityId) -> None:
        await self._client.set(
            self._active_key(uid),
            _MailboxState.INACTIVE.value,
        )
        # Sending a close sentinel to the queue is a quick way to force
        # the entity waiting on messages to the mailbox to stop blocking.
        # This assumes that only one entity is reading from the mailbox.
        await self._client.rpush(self._queue_key(uid), _CLOSE_SENTINEL)  # type: ignore[misc]
        if isinstance(uid, AgentId):
            await self._client.delete(self._behavior_key(uid))


class RedisExchangeFactory(ExchangeFactory[RedisExchangeTransport]):
    """Redis exchange client factory.

    Args:
        hostname: Redis server hostname.
        port: Redis server port.
        redis_kwargs: Extra keyword arguments to pass to
            [`redis.Redis()`][redis.Redis].
    """

    def __init__(
        self,
        hostname: str,
        port: int,
        **redis_kwargs: Any,
    ) -> None:
        self.redis_info = _RedisConnectionInfo(hostname, port, redis_kwargs)

    async def _create_transport(
        self,
        mailbox_id: EntityId | None = None,
        *,
        name: str | None = None,
        registration: RedisAgentRegistration[Any] | None = None,  # type: ignore[override]
    ) -> RedisExchangeTransport:
        return await RedisExchangeTransport.new(
            mailbox_id=mailbox_id,
            name=name,
            redis_info=self.redis_info,
        )
