from __future__ import annotations

import asyncio
import logging
import sys
import time
import uuid
from typing import Any
from typing import Protocol

import redis
import redis.asyncio

from academy.exchange.cloud.client_info import ClientInfo

if sys.version_info >= (3, 13):  # pragma: >=3.13 cover
    from asyncio import Queue
    from asyncio import QueueShutDown

    AsyncQueue = Queue
else:  # pragma: <3.13 cover
    # Use of queues here is isolated to a single thread/event loop so
    # we only need culsans queues for the backport of shutdown() agent
    from culsans import AsyncQueue
    from culsans import AsyncQueueShutDown as QueueShutDown
    from culsans import Queue

from academy.exception import BadEntityIdError
from academy.exception import ForbiddenError
from academy.exception import MailboxTerminatedError
from academy.exception import MessageTooLargeError
from academy.exchange.transport import _respond_pending_requests_on_terminate
from academy.exchange.transport import MailboxStatus
from academy.identifier import AgentId
from academy.identifier import EntityId
from academy.message import Header
from academy.message import Message
from academy.stats import AgentStats

logger = logging.getLogger(__name__)

KB_TO_BYTES = 1024


class MailboxBackend(Protocol):
    """Backend protocol for storing mailboxes on server."""

    async def check_mailbox(
        self,
        client: ClientInfo,
        uid: EntityId,
    ) -> MailboxStatus:
        """Check if a mailbox exists, or is terminated.

        Args:
            client: Client making the request.
            uid: Mailbox id to check.

        Returns:
            The mailbox status.

        Raises:
            ForbiddenError: If the client does not have the right permissions.
        """
        ...

    async def create_mailbox(
        self,
        client: ClientInfo,
        uid: EntityId,
        agent: tuple[str, ...] | None = None,
        permitted_groups: set[str] | None = None,
    ) -> None:
        """Create a mailbox is not exists.

        This method should be idempotent.

        Args:
            client: Client making the request.
            uid: Mailbox id to create.
            agent: Agent class name for agent mailboxes.
            permitted_groups: Groups permitted to access this mailbox.

        Raises:
            ForbiddenError: If the client does not have the right permissions.
        """
        ...

    async def terminate(self, client: ClientInfo, uid: EntityId) -> None:
        """Close a mailbox.

        For security, the manager should keep a gravestone so the same id
        cannot be re-registered.

        Args:
            client: Client making the request.
            uid: Mailbox id to close.

        Raises:
            ForbiddenError: If the client does not have the right permissions.

        """
        ...

    async def discover(
        self,
        client: ClientInfo,
        agent: str,
        allow_subclasses: bool,
    ) -> list[AgentId[Any]]:
        """Find mailboxes of matching agent class.

        Args:
            client: Client making the request.
            agent: Agent class to search for.
            allow_subclasses: Include agents that inherit from the target.

        Raises:
            ForbiddenError: If the client does not have the right permissions.
        """
        ...

    async def get(
        self,
        client: ClientInfo,
        uid: EntityId,
        *,
        timeout: float | None = None,
    ) -> Message[Any]:
        """Get messages from a mailbox.

        Args:
            client: Client making the request.
            uid: Mailbox id to get messages.
            timeout: Time in seconds to wait for message.
                If None, wait indefinitely.

        Raises:
            ForbiddenError: If the client does not have the right permissions.
            BadEntityIdError: The mailbox requested does not exist.
            MailboxTerminatedError: The mailbox is closed.
            TimeoutError: There was not message received during the timeout.
        """
        ...

    async def put(self, client: ClientInfo, message: Message[Any]) -> None:
        """Put a message in a mailbox.

        Args:
            client: Client making the request.
            message: Message to put in mailbox.

        Raises:
            ForbiddenError: If the client does not have the right permissions.
            BadEntityIdError: The mailbox requested does not exist.
            MailboxTerminatedError: The mailbox is closed.
            MessageTooLargeError: The message is larger than the message
                size limit for this exchange.
        """
        ...

    async def share_mailbox(
        self,
        client: ClientInfo,
        uid: EntityId,
        group_uid: str,
    ) -> None:
        """Share a mailbox with a Globus Group.

        Only the owner of the Mailbox is allowed to share with a Globus Group.
        This method should be idempotent.

        Args:
            client: Client making the request.
            uid: Mailbox id to share.
            group_uid: Globus Group id to share.

        Raises:
            ForbiddenError: If the client does not have the right permissions.
            BadEntityIdError: The mailbox to share does not exist.
            MailboxTerminatedError: The mailbox is closed.
        """
        ...

    async def get_mailbox_shares(
        self,
        client: ClientInfo,
        uid: EntityId,
    ) -> list[str]:
        """Get list of globus groups the mailbox is shared with.

        Only the owner of the Mailbox is allowed to share with a Globus Group.
        This method should be idempotent.

        Args:
            client: Client making the request.
            uid: Mailbox id to share.

        Returns:
            List of globus groups id strings

        Raises:
            ForbiddenError: If the client does not have the right permissions.
            BadEntityIdError: The mailbox requested does not exist.
            MailboxTerminatedError: The mailbox is closed.
        """
        ...

    async def remove_mailbox_shares(
        self,
        client: ClientInfo,
        uid: EntityId,
        group_uid: str,
    ) -> None:
        """Stop sharing a mailbox with a group.

        Only the owner of the Mailbox is allowed to share with a Globus Group.
        This method should be idempotent (removing a group that the mailbox
        is not shared with is a no-op).

        Args:
            client: Client making the request.
            uid: Mailbox id to share.
            group_uid: Globus Group id to remove.

        Raises:
            ForbiddenError: If the client does not have the right permissions.
            BadEntityIdError: The mailbox to share does not exist.
            MailboxTerminatedError: The mailbox is closed.
        """
        ...

    async def update_heartbeat(self, uid: EntityId) -> None:
        """Update the heartbeat timestamp for a mailbox.

        Args:
            uid: Mailbox id to update.
        """
        ...

    async def heartbeat_status(self, uid: EntityId) -> float | None:
        """Get the last heartbeat timestamp of a mailbox.

        Args:
            uid: Mailbox id to check.

        Returns:
            Unix timestamp of the last heartbeat, or None if never recorded.
        """
        ...

    async def agent_stats(self, uid: EntityId) -> AgentStats:
        """Return live exchange-level metrics for an agent."""
        ...


class PythonBackend:
    """Mailbox backend using in-memory python data structures.

    Args:
        message_size_limit_kb: Maximum message size to allow.
    """

    def __init__(
        self,
        message_size_limit_kb: int = 1024,
    ) -> None:
        self._owners: dict[EntityId, str | None] = {}
        self._shares: dict[EntityId, set[str]] = {}
        self._mailboxes: dict[EntityId, AsyncQueue[Message[Any]]] = {}
        self._terminated: set[EntityId] = set()
        self._agents: dict[AgentId[Any], tuple[str, ...]] = {}
        self._locks: dict[EntityId, asyncio.Lock] = {}
        self._requests: dict[EntityId, dict[uuid.UUID, Header]] = {}
        self._completions: dict[EntityId, int] = {}
        self._outgoing: dict[EntityId, int] = {}
        self.message_size_limit = message_size_limit_kb * KB_TO_BYTES
        self.last_active: dict[EntityId, float] = {}

    def _has_permissions(self, client: ClientInfo, uid: EntityId) -> bool:
        """Check if a user has permission to share mailbox.

        Args:
            client: Client making the request.
            uid: EntityID to check perms for
        """
        return self._has_mailbox_ownership(
            client,
            uid,
        ) or self._has_shared_mailbox_access(client, uid)

    def _shared_groups(self, uid: EntityId) -> set[str]:
        """Get the set of Globus groups this mailbox is shared with."""
        return self._shares.get(uid, set())

    def _has_shared_mailbox_access(
        self,
        client: ClientInfo,
        uid: EntityId,
    ) -> bool:
        """Check if the mailbox is shared with user via Globus groups."""
        return not client.group_memberships.isdisjoint(
            self._shared_groups(uid),
        )

    def _has_mailbox_ownership(
        self,
        client: ClientInfo,
        entity: EntityId,
    ) -> bool:
        return (
            entity not in self._owners
            or self._owners[entity] == client.client_id
        )

    async def check_mailbox(
        self,
        client: ClientInfo,
        uid: EntityId,
    ) -> MailboxStatus:
        """Check if a mailbox exists, or is terminated.

        Args:
            client: Client making the request.
            uid: Mailbox id to check.

        Returns:
            The mailbox status.

        Raises:
            ForbiddenError: If the client does not have the right permissions.
        """
        if uid not in self._mailboxes:
            return MailboxStatus.MISSING
        elif not self._has_permissions(client, uid):
            raise ForbiddenError(
                'Client does not have correct permissions.',
            )

        async with self._locks[uid]:
            if uid in self._terminated:
                return MailboxStatus.TERMINATED
            else:
                return MailboxStatus.ACTIVE

    async def create_mailbox(
        self,
        client: ClientInfo,
        uid: EntityId,
        agent: tuple[str, ...] | None = None,
        permitted_groups: set[str] | None = None,
    ) -> None:
        """Create a mailbox is not exists.

        This method should be idempotent.

        Args:
            client: Client making the request.
            uid: Mailbox id to create.
            agent: Agent class name for agent mailboxes.
            permitted_groups: Groups permitted to access this mailbox.

        Raises:
            ForbiddenError: If the client does not have the right permissions.
        """
        if not self._has_permissions(client, uid):
            raise ForbiddenError(
                'Client does not have correct permissions.',
            )

        mailbox = self._mailboxes.get(uid, None)
        if mailbox is None:
            if sys.version_info >= (3, 13):  # pragma: >=3.13 cover
                queue: AsyncQueue[Message[Any]] = Queue()
            else:  # pragma: <3.13 cover
                queue: AsyncQueue[Message[Any]] = Queue().async_q
            self._mailboxes[uid] = queue
            self._terminated.discard(uid)
            self._owners[uid] = client.client_id
            self._locks[uid] = asyncio.Lock()
            if agent is not None and isinstance(uid, AgentId):
                self._agents[uid] = agent
            if permitted_groups:
                self._shares[uid] = permitted_groups
            logger.info(
                'Created mailbox for %s',
                uid,
                extra={'academy.mailbox_id': uid},
            )

    async def terminate(self, client: ClientInfo, uid: EntityId) -> None:
        """Close a mailbox.

        For security, the manager should keep a gravestone so the same id
        cannot be re-registered.

        Args:
            client: Client making the request.
            uid: Mailbox id to close.

        Raises:
            ForbiddenError: If the client does not have the right permissions.
        """
        if not self._has_mailbox_ownership(client, uid):
            raise ForbiddenError(
                'Client does not have correct permissions.',
            )

        self._terminated.add(uid)
        mailbox = self._mailboxes.get(uid, None)
        if mailbox is None:
            return

        async def send(message: Message[Any]) -> None:
            await self.put(client, message)

        pending_requests: list[Header] | None = None
        async with self._locks[uid]:
            pending_requests = list(self._requests.pop(uid, {}).values())
            mailbox.shutdown(immediate=True)

        await _respond_pending_requests_on_terminate(
            pending_requests,
            send,
        )

    async def update_heartbeat(self, uid: EntityId) -> None:
        """Update the heartbeat timestamp for a mailbox."""
        self.last_active[uid] = time.time()

    async def heartbeat_status(self, uid: EntityId) -> float | None:
        """Get the last heartbeat timestamp for a mailbox."""
        if uid not in self._mailboxes:
            raise BadEntityIdError(uid)

        if self.last_active.get(uid) is None:
            return None

        return time.time() - self.last_active[uid]

    async def agent_stats(self, uid: EntityId) -> AgentStats:
        """Return live exchange-level metrics for an agent."""
        queue = self._mailboxes.get(uid)
        queued = queue.qsize() if queue is not None else 0
        pending = len(self._requests.get(uid, {}))
        completed = self._completions.get(uid, 0)
        outgoing = self._outgoing.get(uid, 0)
        in_progress = max(0, pending - queued)
        incoming = completed + pending
        return AgentStats(
            incoming=incoming,
            outgoing=outgoing,
            completed=completed,
            in_progress=in_progress,
            queued=queued,
        )

    async def discover(
        self,
        client: ClientInfo,
        agent: str,
        allow_subclasses: bool,
    ) -> list[AgentId[Any]]:
        """Find mailboxes of matching agent class.

        Args:
            client: Client making the request.
            agent: Agent class to search for.
            allow_subclasses: Include agents that inherit from the target.

        Raises:
            ForbiddenError: If the client does not have the right permissions.
        """
        found: list[AgentId[Any]] = []
        for aid, agents in self._agents.items():
            if not self._has_permissions(client, aid):
                continue
            if aid in self._terminated:
                continue
            if agent == agents[0] or (allow_subclasses and agent in agents):
                found.append(aid)
        return found

    async def get(
        self,
        client: ClientInfo,
        uid: EntityId,
        *,
        timeout: float | None = None,
    ) -> Message[Any]:
        """Get messages from a mailbox.

        Args:
            client: Client making the request.
            uid: Mailbox id to get messages.
            timeout: Time in seconds to wait for message.
                If None, wait indefinitely.

        Raises:
            ForbiddenError: If the client does not have the right permissions.
            BadEntityIdError: The mailbox requested does not exist.
            MailboxTerminatedError: The mailbox is closed.
            TimeoutError: There was not message received during the timeout.
        """
        if not self._has_permissions(client, uid):
            raise ForbiddenError(
                'Client does not have correct permissions.',
            )

        try:
            queue = self._mailboxes[uid]
        except KeyError as e:
            raise BadEntityIdError(uid) from e
        try:
            return await asyncio.wait_for(queue.get(), timeout=timeout)
        except QueueShutDown:
            raise MailboxTerminatedError(uid) from None
        except asyncio.TimeoutError:
            # In Python 3.10 and older, asyncio.TimeoutError and TimeoutError
            # are different error types.
            raise TimeoutError(
                f'No message retrieved within {timeout} seconds.',
            ) from None

    def _track_put_delivery(
        self,
        message: Message[Any],
        matched_request_header: Header | None,
    ) -> None:
        if message.is_request():
            self._requests.setdefault(message.dest, {})[message.tag] = (
                message.header
            )
            self._outgoing[message.src] = (
                self._outgoing.get(message.src, 0) + 1
            )
            logger.debug(
                'Tracking in-flight request: tag=%s src=%s dest=%s',
                message.tag,
                message.src,
                message.dest,
                extra={
                    'academy.message_tag': message.tag,
                    'academy.src': message.src,
                    'academy.dest': message.dest,
                },
            )
        elif matched_request_header is not None:
            self._requests[message.src].pop(message.tag)
            if not self._requests[message.src]:
                del self._requests[message.src]
            self._completions[message.src] = (
                self._completions.get(message.src, 0) + 1
            )
            logger.debug(
                'Response received for in-flight request: '
                'tag=%s src=%s dest=%s',
                matched_request_header.tag,
                matched_request_header.src,
                matched_request_header.dest,
                extra={
                    'academy.message_tag': matched_request_header.tag,
                    'academy.src': matched_request_header.src,
                    'academy.dest': matched_request_header.dest,
                },
            )
        else:
            logger.warning(
                'Response received without corresponding request: '
                'tag=%s src=%s dest=%s',
                message.tag,
                message.src,
                message.dest,
                extra={
                    'academy.message_tag': message.tag,
                    'academy.src': message.src,
                    'academy.dest': message.dest,
                },
            )

    async def put(self, client: ClientInfo, message: Message[Any]) -> None:
        """Put a message in a mailbox.

        Args:
            client: Client making the request.
            message: Message to put in mailbox.

        Raises:
            ForbiddenError: If the client does not have the right permissions.
            BadEntityIdError: The mailbox requested does not exist.
            MailboxTerminatedError: The mailbox is closed.
            MessageTooLargeError: The message is larger than the message
                size limit for this exchange.
        """
        # For responses, check for a matching pending request before
        # the permission check. If a match is found, the response
        # corresponds to a request that was already authorized when it
        # was delivered, so the destination permission check is skipped.
        # This closes the detachment between group-based authorization
        # on requests (checked against the agent's mailbox shares) and
        # mailbox-based authorization on responses (which would check
        # against the sender's mailbox shares and fail when those are
        # empty).
        matched_request_header: Header | None = None
        if message.is_response():
            pending = self._requests.get(message.src, {})
            matched_request_header = pending.get(message.tag)

        if matched_request_header is None and not self._has_permissions(
            client,
            message.dest,
        ):
            raise ForbiddenError(
                'Client does not have correct permissions.',
            )

        if not self._has_mailbox_ownership(client, message.src):
            raise ForbiddenError(
                'Sender does not own source mailbox.',
            )

        if matched_request_header is not None:
            sender_groups = matched_request_header.groups
        else:
            sender_groups = frozenset(
                client.group_memberships
                & self._shared_groups(
                    message.dest,
                ),
            )
        message.header = message.header.model_copy(
            update={'groups': sender_groups},
        )

        if sys.getsizeof(message.body) > self.message_size_limit:
            raise MessageTooLargeError(
                sys.getsizeof(message.body),
                self.message_size_limit,
            )

        try:
            queue = self._mailboxes[message.dest]
        except KeyError as e:
            raise BadEntityIdError(message.dest) from e

        self._track_put_delivery(message, matched_request_header)

        async with self._locks[message.dest]:
            try:
                await queue.put(message)
            except QueueShutDown:
                raise MailboxTerminatedError(message.dest) from None

    async def share_mailbox(
        self,
        client: ClientInfo,
        uid: EntityId,
        group_uid: str,
    ) -> None:
        """Share a mailbox with a Globus group.

        Args:
            client: Client making the request.
            uid: Target Mailbox for sharing
            group_uid: Group id to share mailbox with.

        Raises:
            ForbiddenError: If the client does not have the right permissions.
        """
        if uid not in self._mailboxes:
            raise BadEntityIdError(uid)

        if uid in self._terminated:
            raise MailboxTerminatedError(uid)

        if not self._has_mailbox_ownership(client, uid):
            raise ForbiddenError(
                f'{client.client_id} cannot share mailbox '
                f'{uid} it does not own.',
            )

        if group_uid not in client.group_memberships:
            raise ForbiddenError(
                f'Owner does not belong to the group {group_uid}',
            )

        if uid not in self._shares:
            self._shares[uid] = set()

        self._shares[uid].add(group_uid)

    async def get_mailbox_shares(
        self,
        client: ClientInfo,
        uid: EntityId,
    ) -> list[str]:
        """Get list of globus groups the mailbox is shared with.

        Only the owner of the Mailbox is allowed to share with a Globus Group.
        This method should be idempotent.

        Args:
            client: Client making the request.
            uid: Mailbox id to share.

        Returns:
            List of globus groups id strings

        Raises:
            ForbiddenError: If the client does not have the right permissions.
            BadEntityIdError: The mailbox requested does not exist.
            MailboxTerminatedError: The mailbox is closed.
        """
        if uid not in self._mailboxes:
            raise BadEntityIdError(uid)

        if uid in self._terminated:
            raise MailboxTerminatedError(uid)

        if not self._has_mailbox_ownership(client, uid):
            raise ForbiddenError(
                'Viewing shared groups requires ownership',
            )

        return list(self._shares.get(uid, set()))

    async def remove_mailbox_shares(
        self,
        client: ClientInfo,
        uid: EntityId,
        group_uid: str,
    ) -> None:
        """Stop sharing a mailbox with a group.

        Only the owner of the Mailbox is allowed to share with a Globus Group.
        This method should be idempotent (removing a group that the mailbox
        is not shared with is a no-op).

        Args:
            client: Client making the request.
            uid: Mailbox id to share.
            group_uid: Globus Group id to remove.

        Raises:
            ForbiddenError: If the client does not have the right permissions.
            BadEntityIdError: The mailbox to share does not exist.
            MailboxTerminatedError: The mailbox is closed.
        """
        ...

        if uid not in self._mailboxes:
            raise BadEntityIdError(uid)

        if uid in self._terminated:
            raise MailboxTerminatedError(uid)

        if not self._has_mailbox_ownership(client, uid):
            raise ForbiddenError(
                f'{client.client_id} cannot change permissions on'
                f'mailbox {uid} it does not own.',
            )

        self._shares[uid].discard(group_uid)
        logger.info('Group %s removed from mailbox %s', group_uid, uid)


_CLOSE_SENTINEL = b'<CLOSED>'
_OWNER_SUFFIX = '_'


class RedisBackend:
    """Redis backend of mailboxes.

    Args:
        hostname: Host address of redis.
        port: Redis port.
        message_size_limit_kb: Maximum message size to allow.
        kwargs: Addition arguments to pass to redis session.
    """

    def __init__(  # noqa: PLR0913
        self,
        hostname: str = 'localhost',
        port: int = 6379,
        *,
        message_size_limit_kb: int = 1024,
        kwargs: dict[str, Any] | None = None,
        mailbox_expiration_s: int | None = None,
        gravestone_expiration_s: int | None = None,
    ) -> None:
        self.message_size_limit = message_size_limit_kb * KB_TO_BYTES

        if kwargs is None:  # pragma: no branch
            kwargs = {}

        self._client: redis.asyncio.Redis[bytes] = redis.asyncio.Redis(
            host=hostname,
            port=port,
            decode_responses=False,
            **kwargs,
        )
        self.mailbox_expiration_s = mailbox_expiration_s
        self.gravestone_expiration_s = gravestone_expiration_s

    def _owner_key(self, uid: EntityId) -> str:
        return f'owner:{uid.uid}'

    def _active_key(self, uid: EntityId) -> str:
        return f'active:{uid.uid}'

    def _agent_key(self, uid: EntityId) -> str:
        return f'agent:{uid.uid}'

    def _queue_key(self, uid: EntityId) -> str:
        return f'queue:{uid.uid}'

    def _share_key(self, uid: EntityId) -> str:
        return f'share:{uid.uid}'

    def _request_key(self, uid: EntityId, tag: uuid.UUID) -> str:
        return f'request:{uid.uid}:{tag}'

    def _heartbeat_key(self, uid: EntityId) -> str:
        return f'heartbeat:{uid.uid}'

    def _completions_key(self, uid: EntityId) -> str:
        return f'completions:{uid.uid}'

    def _outgoing_key(self, uid: EntityId) -> str:
        return f'outgoing:{uid.uid}'

    async def _has_permissions(
        self,
        client: ClientInfo,
        entity: EntityId,
    ) -> bool:
        owns = await self._has_mailbox_ownership(client, entity)
        groups = await self._has_shared_mailbox_access(client, entity)
        return owns or groups

    async def _shared_groups(self, uid: EntityId) -> set[str]:
        """Get the set of Globus groups this mailbox is shared with."""
        _groups = await self._client.smembers(self._share_key(uid))
        return {g.decode() for g in _groups}

    async def _has_shared_mailbox_access(
        self,
        client: ClientInfo,
        uid: EntityId,
    ) -> bool:
        groups = await self._shared_groups(uid)
        return not client.group_memberships.isdisjoint(groups)

    async def _has_mailbox_ownership(
        self,
        client: ClientInfo,
        entity: EntityId,
    ) -> bool:
        owner = await self._client.get(
            self._owner_key(entity),
        )
        return (
            owner is None
            or owner.decode() == f'{client.client_id}{_OWNER_SUFFIX}'
        )

    async def _update_expirations(
        self,
        entity: EntityId,
    ) -> None:
        if self.gravestone_expiration_s is None:
            return

        await self._client.expire(
            self._active_key(entity),
            self.gravestone_expiration_s,
        )
        await self._client.expire(
            self._owner_key(entity),
            self.gravestone_expiration_s,
        )
        await self._client.expire(
            self._heartbeat_key(entity),
            self.gravestone_expiration_s,
        )
        if isinstance(entity, AgentId):
            await self._client.expire(
                self._agent_key(entity),
                self.gravestone_expiration_s,
            )

    async def check_mailbox(
        self,
        client: ClientInfo,
        uid: EntityId,
    ) -> MailboxStatus:
        """Check if a mailbox exists, or is terminated.

        Args:
            client: Client making the request.
            uid: Mailbox id to check.

        Returns:
            The mailbox status.

        Raises:
            ForbiddenError: If the client does not have the right permissions.
        """
        if not await self._has_permissions(client, uid):
            raise ForbiddenError(
                'Client does not have correct permissions.',
            )

        status = await self._client.get(self._active_key(uid))
        if status is None:
            return MailboxStatus.MISSING
        elif status.decode() == MailboxStatus.TERMINATED.value:
            return MailboxStatus.TERMINATED
        else:
            return MailboxStatus.ACTIVE

    async def create_mailbox(
        self,
        client: ClientInfo,
        uid: EntityId,
        agent: tuple[str, ...] | None = None,
        permitted_groups: set[str] | None = None,
    ) -> None:
        """Create a mailbox is not exists.

        This method should be idempotent.

        Args:
            client: Client making the request.
            uid: Mailbox id to create.
            agent: Agent class name for agent mailboxes.
            permitted_groups: Groups permitted to access this mailbox.

        Raises:
            ForbiddenError: If the client does not have the right permissions.
        """
        if not await self._has_permissions(client, uid):
            raise ForbiddenError(
                'Client does not have correct permissions.',
            )

        await self._client.set(
            self._active_key(uid),
            MailboxStatus.ACTIVE.value,
        )

        if agent is not None:
            await self._client.set(
                self._agent_key(uid),
                ','.join(agent),
            )

        created = await self._client.set(
            self._owner_key(uid),
            f'{client.client_id}{_OWNER_SUFFIX}',
            nx=True,
        )
        if created and permitted_groups:
            await self._client.sadd(
                self._share_key(uid),
                *permitted_groups,
            )
        await self._update_expirations(uid)

    async def terminate(self, client: ClientInfo, uid: EntityId) -> None:
        """Close a mailbox.

        For security, the manager should keep a gravestone so the same id
        cannot be re-registered.

        Args:
            client: Client making the request.
            uid: Mailbox id to close.

        Raises:
            ForbiddenError: If the client does not have the right permissions.
        """
        if not await self._has_mailbox_ownership(client, uid):
            raise ForbiddenError(
                'Client does not have correct permissions.',
            )

        status = await self.check_mailbox(client, uid)

        if status in {MailboxStatus.MISSING, MailboxStatus.TERMINATED}:
            return

        await self._client.set(
            self._active_key(uid),
            MailboxStatus.TERMINATED.value,
        )

        if self.gravestone_expiration_s is not None:
            await self._client.expire(
                self._active_key(uid),
                self.gravestone_expiration_s,
            )

        await self._client.delete(self._queue_key(uid))
        # Sending a close sentinel to the queue is a quick way to force
        # the entity waiting on messages to the mailbox to stop blocking.
        # This assumes that only one entity is reading from the mailbox.
        await self._client.rpush(self._queue_key(uid), _CLOSE_SENTINEL)
        if isinstance(uid, AgentId):
            await self._client.delete(self._agent_key(uid))

        req_keys = [
            key
            async for key in self._client.scan_iter(
                f'request:{uid.uid}:*',
                count=1000,
            )
        ]
        pending_requests: list[Header] = []
        for req_key in req_keys:
            data = await self._client.get(req_key)
            if data is None:  # The key was removed between scan and delete
                continue  # pragma: no cover
            pending_requests.append(Header.model_validate_json(data.decode()))
            await self._client.delete(req_key)

        async def send(message: Message[Any]) -> None:
            await self.put(client, message)

        await _respond_pending_requests_on_terminate(
            pending_requests,
            send,
        )

    async def _redis_current_time(self) -> float:
        """Helper to transform Redis time structure to Unix float.

        Returns:
            Unix timestamp as a float

        """
        # Returns in the form [seconds since epoch, microseconds]
        current_time = await self._client.time()

        current_seconds = int(current_time[0])
        current_microseconds = int(current_time[1]) / 1000000

        now = current_seconds + current_microseconds

        return now

    async def update_heartbeat(self, uid: EntityId) -> None:
        """Update the heartbeat timestamp for a mailbox."""
        now = await self._redis_current_time()

        await self._client.set(self._heartbeat_key(uid), str(now))
        await self._update_expirations(uid)

    async def heartbeat_status(self, uid: EntityId) -> float | None:
        """Get the time since last heartbeat timestamp for a mailbox."""
        status = await self._client.get(self._active_key(uid))
        if status is None:
            raise BadEntityIdError(uid)

        heartbeat_time = await self._client.get(self._heartbeat_key(uid))

        if heartbeat_time is None:
            return None

        now = await self._redis_current_time()

        return now - float(heartbeat_time.decode())

    async def agent_stats(self, uid: EntityId) -> AgentStats:
        """Return live exchange-level metrics for an agent."""
        queued = await self._client.llen(self._queue_key(uid))
        pending = 0
        async for _ in self._client.scan_iter(
            f'request:{uid.uid}:*',
            count=1000,
        ):
            pending += 1
        completed_raw = await self._client.get(self._completions_key(uid))
        outgoing_raw = await self._client.get(self._outgoing_key(uid))
        completed = int(completed_raw) if completed_raw is not None else 0
        outgoing = int(outgoing_raw) if outgoing_raw is not None else 0
        in_progress = max(0, pending - queued)
        incoming = completed + pending
        return AgentStats(
            incoming=incoming,
            outgoing=outgoing,
            completed=completed,
            in_progress=in_progress,
            queued=queued,
        )

    async def discover(
        self,
        client: ClientInfo,
        agent: str,
        allow_subclasses: bool,
    ) -> list[AgentId[Any]]:
        """Find mailboxes of matching agent class.

        Args:
            client: Client making the request.
            agent: Agent class to search for.
            allow_subclasses: Include agents that inherit from the target.
        """
        found: list[AgentId[Any]] = []
        async for key in self._client.scan_iter(
            'agent:*',
            count=1000,
        ):  # pragma: no branch
            data = await self._client.get(key)
            if data is None:  # Key was removed between scan and read
                continue  # pragma: no cover
            mro_str = data.decode()
            assert isinstance(mro_str, str)
            mro = mro_str.split(',')
            if agent == mro[0] or (allow_subclasses and agent in mro):
                aid: AgentId[Any] = AgentId(
                    uid=uuid.UUID(key.decode().split(':')[-1]),
                )
                found.append(aid)

        active: list[AgentId[Any]] = []
        for aid in found:
            if await self._has_permissions(client, aid):
                status = await self._client.get(self._active_key(aid))
                if (
                    status is not None
                    and status.decode() == MailboxStatus.ACTIVE.value
                ):  # pragma: no branch
                    active.append(aid)

        return active

    async def get(
        self,
        client: ClientInfo,
        uid: EntityId,
        *,
        timeout: float | None = None,
    ) -> Message[Any]:
        """Get messages from a mailbox.

        Args:
            client: Client making the request.
            uid: Mailbox id to get messages.
            timeout: Time in seconds to wait for message.
                If None, wait indefinitely.

        Raises:
            ForbiddenError: If the client does not have the right permissions.
            BadEntityIdError: The mailbox requested does not exist.
            MailboxTerminatedError: The mailbox is closed.
            TimeoutError: There was not message received during the timeout.
        """
        if not await self._has_permissions(client, uid):
            raise ForbiddenError(
                'Client does not have correct permissions.',
            )

        _timeout = timeout if timeout is not None else 0
        status = await self._client.get(
            self._active_key(uid),
        )
        if status is None:
            raise BadEntityIdError(uid)
        elif status.decode() == MailboxStatus.TERMINATED.value:
            raise MailboxTerminatedError(uid)

        await self._update_expirations(uid)
        if self.mailbox_expiration_s:
            await self._client.expire(
                self._queue_key(uid),
                self.mailbox_expiration_s,
            )

        raw = await self._client.blpop(
            [self._queue_key(uid)],
            timeout=_timeout,
        )
        if raw is None:
            raise TimeoutError(
                f'Timeout waiting for next message for {uid} '
                f'after {timeout} seconds.',
            )

        # Only passed one key to blpop to result is [key, item]
        assert len(raw) == 2  # noqa: PLR2004
        if raw[1] == _CLOSE_SENTINEL:  # pragma: no cover
            raise MailboxTerminatedError(uid)
        return Message.model_deserialize(raw[1])

    async def _track_put_delivery(
        self,
        message: Message[Any],
        matched_request_header: Header | None,
    ) -> None:
        if message.is_request():
            await self._client.set(
                self._request_key(message.dest, message.tag),
                message.header.model_dump_json(),
            )
            await self._client.incr(self._outgoing_key(message.src))
            if self.mailbox_expiration_s:
                await self._client.expire(
                    self._request_key(message.dest, message.tag),
                    self.mailbox_expiration_s,
                )
            logger.debug(
                'Tracking in-flight request: tag=%s src=%s dest=%s',
                message.tag,
                message.src,
                message.dest,
                extra={
                    'academy.message_tag': message.tag,
                    'academy.src': message.src,
                    'academy.dest': message.dest,
                },
            )
        elif matched_request_header is not None:
            req_key = self._request_key(message.src, message.tag)
            await self._client.delete(req_key)
            await self._client.incr(self._completions_key(message.src))
            logger.debug(
                'Response received for in-flight request: '
                'tag=%s src=%s dest=%s',
                matched_request_header.tag,
                matched_request_header.src,
                matched_request_header.dest,
                extra={
                    'academy.message_tag': matched_request_header.tag,
                    'academy.src': matched_request_header.src,
                    'academy.dest': matched_request_header.dest,
                },
            )
        else:
            logger.warning(
                'Response received without corresponding request: '
                'tag=%s src=%s dest=%s',
                message.tag,
                message.src,
                message.dest,
                extra={
                    'academy.message_tag': message.tag,
                    'academy.src': message.src,
                    'academy.dest': message.dest,
                },
            )

    async def put(self, client: ClientInfo, message: Message[Any]) -> None:
        """Put a message in a mailbox.

        Args:
            client: Client making the request.
            message: Message to put in mailbox.

        Raises:
            ForbiddenError: If the client does not have the right permissions.
            BadEntityIdError: The mailbox requested does not exist.
            MailboxTerminatedError: The mailbox is closed.
            MessageTooLargeError: The message is larger than the message
                size limit for this exchange.
        """
        # For responses, check for a matching pending request before
        # the permission check. If a match is found, the response
        # corresponds to a request that was already authorized when it
        # was delivered, so the destination permission check is skipped.
        # This closes the detachment between group-based authorization
        # on requests (checked against the agent's mailbox shares) and
        # mailbox-based authorization on responses (which would check
        # against the sender's mailbox shares and fail when those are
        # empty).
        matched_request_header: Header | None = None
        if message.is_response():
            req_key = self._request_key(message.src, message.tag)
            matching_data = await self._client.get(req_key)
            if matching_data is not None:
                matched_request_header = Header.model_validate_json(
                    matching_data.decode(),
                )

        if matched_request_header is None and not await self._has_permissions(
            client,
            message.dest,
        ):
            raise ForbiddenError(
                'Client does not have correct permissions.',
            )

        if not await self._has_mailbox_ownership(client, message.src):
            raise ForbiddenError(
                'Sender does not own source mailbox.',
            )

        if matched_request_header is not None:
            sender_groups = matched_request_header.groups
        else:
            sender_groups = frozenset(
                client.group_memberships
                & await self._shared_groups(message.dest),
            )
        message.header = message.header.model_copy(
            update={'groups': sender_groups},
        )

        status = await self._client.get(self._active_key(message.dest))
        if status is None:
            raise BadEntityIdError(message.dest)
        elif status.decode() == MailboxStatus.TERMINATED.value:
            raise MailboxTerminatedError(message.dest)

        serialized = message.model_serialize()
        if len(serialized) > self.message_size_limit:
            raise MessageTooLargeError(
                len(serialized),
                self.message_size_limit,
            )

        await self._track_put_delivery(message, matched_request_header)

        await self._client.rpush(
            self._queue_key(message.dest),
            serialized,
        )

        if self.mailbox_expiration_s:
            await self._client.expire(
                self._queue_key(message.dest),
                self.mailbox_expiration_s,
                nx=True,
            )

    async def share_mailbox(
        self,
        client: ClientInfo,
        uid: EntityId,
        group_uid: str,
    ) -> None:
        """Share a mailbox with a Globus group.

        Args:
             client: Client making the request.
             group_uid: Group id to share mailbox with.
             uid: Target Mailbox for sharing

        Raises:
            ForbiddenError: If the client does not have the right permissions.
        """
        status = await self._client.get(self._active_key(uid))
        if status is None:
            raise BadEntityIdError(uid)
        elif status.decode() == MailboxStatus.TERMINATED.value:
            raise MailboxTerminatedError(uid)

        if not await self._has_mailbox_ownership(client, uid):
            raise ForbiddenError(
                f'{client.client_id} cannot share mailbox '
                f'{uid} it does not own.',
            )
        if group_uid not in client.group_memberships:
            raise ForbiddenError(
                f'Owner does not belong to the group {group_uid}',
            )

        await self._client.sadd(self._share_key(uid), group_uid)

    async def get_mailbox_shares(
        self,
        client: ClientInfo,
        uid: EntityId,
    ) -> list[str]:
        """Get list of globus groups the mailbox is shared with.

        Only the owner of the Mailbox is allowed to share with a Globus Group.
        This method should be idempotent.

        Args:
            client: Client making the request.
            uid: Mailbox id to share.

        Returns:
            List of globus groups id strings

        Raises:
            ForbiddenError: If the client does not have the right permissions.
            BadEntityIdError: The mailbox requested does not exist.
            MailboxTerminatedError: The mailbox is closed.
        """
        status = await self._client.get(self._active_key(uid))
        if status is None:
            raise BadEntityIdError(uid)
        elif status.decode() == MailboxStatus.TERMINATED.value:
            raise MailboxTerminatedError(uid)

        if not await self._has_mailbox_ownership(client, uid):
            raise ForbiddenError(
                'Viewing shared groups requires ownership',
            )

        _groups = await self._client.smembers(self._share_key(uid))
        groups = [g.decode() for g in _groups]
        return groups

    async def remove_mailbox_shares(
        self,
        client: ClientInfo,
        uid: EntityId,
        group_uid: str,
    ) -> None:
        """Stop sharing a mailbox with a group.

        Only the owner of the Mailbox is allowed to share with a Globus Group.
        This method should be idempotent (removing a group that the mailbox
        is not shared with is a no-op).

        Args:
            client: Client making the request.
            uid: Mailbox id to share.
            group_uid: Globus Group id to remove.

        Raises:
            ForbiddenError: If the client does not have the right permissions.
            BadEntityIdError: The mailbox to share does not exist.
            MailboxTerminatedError: The mailbox is closed.
        """
        status = await self._client.get(self._active_key(uid))
        if status is None:
            raise BadEntityIdError(uid)
        elif status.decode() == MailboxStatus.TERMINATED.value:
            raise MailboxTerminatedError(uid)

        if not await self._has_mailbox_ownership(client, uid):
            raise ForbiddenError(
                f'{client.client_id} cannot change permissions on'
                f'mailbox {uid} it does not own.',
            )

        await self._client.srem(self._share_key(uid), group_uid)
