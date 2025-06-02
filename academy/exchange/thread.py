from __future__ import annotations

import logging
import pickle
import threading
from typing import Any, Callable
from typing import TypeVar
import uuid

from academy.behavior import Behavior
from academy.exception import BadEntityIdError
from academy.exception import MailboxClosedError
from academy.exchange import ExchangeMixin
from academy.exchange.queue import Queue
from academy.exchange.queue import QueueClosedError
from academy.handle import BoundRemoteHandle
from academy.identifier import AgentId
from academy.identifier import ClientId
from academy.identifier import EntityId
from academy.message import Message, RequestMessage
from academy.serialize import NoPickleMixin

logger = logging.getLogger(__name__)

BehaviorT = TypeVar('BehaviorT', bound=Behavior)


class ThreadExchange(NoPickleMixin):
    """Local process message exchange for threaded agents.

    ThreadExchange is a special case of an exchange where the mailboxes
    of the exchange live in process memory. This class stores the state
    of the exchange. 
    """

    def __init__(self) -> None:
        self.queues: dict[EntityId, Queue[Message]] = {}
        self.behaviors: dict[AgentId[Any], type[Behavior]] = {}

    def __getstate__(self) -> None:
        raise pickle.PicklingError(
            f'{type(self).__name__} cannot be safely pickled.',
        )

class UnboundThreadExchangeClient:
    def __init__(self, exchange_state: ThreadExchange | None = None):
        if exchange_state is None:
            exchange_state = ThreadExchange()

        self._state = exchange_state

    def bind(
        self, 
        mailbox_id: EntityId | None = None, 
        name: str | None = None, 
        handler: Callable[[RequestMessage], None] | None = None
    ) -> BoundThreadExchangeClient:
        """Bind exchange to client or agent.

        If no agent is provided, exchange should create a new mailbox without
        an associated behavior and bind to that. Otherwise, the exchange will 
        bind to the mailbox associated with the provided agent.

        Note:
            This is intentionally restrictive. Each user or agent should only
            bind to the exchange with a single address. This forces multiplexing
            of handles to other agents and requests to this agents.
        """

        return BoundThreadExchangeClient(self._state, mailbox_id, name, handler)

class BoundThreadExchangeClient(ExchangeMixin):

    def __init__(self, 
                 exchange_state : ThreadExchange, 
                 entity_id: EntityId | None = None,
                 name: str | None = None,
                 handler: Callable[[RequestMessage], None] | None = None):
        
        self._state = exchange_state
        self.bound_handles: dict[uuid.UUID, BoundRemoteHandle[Any]] = {}
        
        if entity_id is None:
            cid = ClientId.new(name=name)
            self._state.queues[cid] = Queue()
            self.mailbox_id = cid
            logger.debug('Registered %s in %s', cid, self)
        else:
            queue = self._state.queues.get(entity_id, None)
            if queue is None:
                raise BadEntityIdError(entity_id)
            self.mailbox_id = entity_id

        self._queue = self._state.queues[cid] 
        self.request_handler = handler

        if handler is None:
            self._listener_thread = threading.Thread(
                target=self.listen,
                name=f'thread-exchange-{self.mailbox_id.uid}-listener',
            )
            self._listener_thread.start()

    def close(self) -> None:
        """Close the exchange.

        Unlike most exchange clients, this will close all of the mailboxes.
        """

        # If does not process requests, terminate it.
        if self.request_handler is None:
            self.terminate(self.mailbox_id)

        for queue in self._state.queues.values():
            queue.close()
        logger.debug('Closed exchange (%s)', self)

    def register_agent(
        self,
        behavior: type[BehaviorT],
        *,
        agent_id: AgentId[BehaviorT] | None = None,
        name: str | None = None,
    ) -> AgentId[BehaviorT]:
        """Create a new agent identifier and associated mailbox.

        Args:
            behavior: Type of the behavior this agent will implement.
            agent_id: Specify the ID of the agent. Randomly generated
                default.
            name: Optional human-readable name for the agent. Ignored if
                `agent_id` is provided.

        Returns:
            Unique identifier for the agent's mailbox.
        """
        aid = AgentId.new(name=name) if agent_id is None else agent_id
        if aid not in self._state.queues or self._state.queues[aid].closed():
            self._state.queues[aid] = Queue()
            self._state.behaviors[aid] = behavior
            logger.debug('Registered %s in %s', aid, self)
        return aid

    def terminate(self, uid: EntityId) -> None:
        """Close the mailbox for an entity from the exchange.

        Note:
            This method is a no-op if the mailbox does not exists.

        Args:
            uid: Entity identifier of the mailbox to close.
        """
        queue = self._state.queues.get(uid, None)
        if queue is not None and not queue.closed():
            queue.close()
            if isinstance(uid, AgentId):
                self._state.behaviors.pop(uid, None)
            logger.debug('Closed mailbox for %s (%s)', uid, self)

    def discover(
        self,
        behavior: type[Behavior],
        *,
        allow_subclasses: bool = True,
    ) -> tuple[AgentId[Any], ...]:
        """Discover peer agents with a given behavior.

        Args:
            behavior: Behavior type of interest.
            allow_subclasses: Return agents implementing subclasses of the
                behavior.

        Returns:
            Tuple of agent IDs implementing the behavior.
        """
        found: list[AgentId[Any]] = []
        for aid, type_ in self._state.behaviors.items():
            if behavior is type_ or (
                allow_subclasses and issubclass(type_, behavior)
            ):
                found.append(aid)
        alive = tuple(aid for aid in found if not self._state.queues[aid].closed())
        return alive

    def send(self, uid: EntityId, message: Message) -> None:
        """Send a message to a mailbox.

        Args:
            uid: Destination address of the message.
            message: Message to send.

        Raises:
            BadEntityIdError: if a mailbox for `uid` does not exist.
            MailboxClosedError: if the mailbox was closed.
        """
        queue = self._state.queues.get(uid, None)
        if queue is None:
            raise BadEntityIdError(uid)
        try:
            queue.put(message)
            logger.debug('Sent %s to %s', type(message).__name__, uid)
        except QueueClosedError as e:
            raise MailboxClosedError(uid) from e

    def recv(self, timeout: float | None = None) -> Message:
        """Receive the next message in the mailbox.

        This blocks until the next message is received or the mailbox
        is closed.

        Args:
            timeout: Optional timeout in seconds to wait for the next
                message. If `None`, the default, block forever until the
                next message or the mailbox is closed.

        Raises:
            MailboxClosedError: if the mailbox was closed.
            TimeoutError: if a `timeout` was specified and exceeded.
        """
        try:
            message = self._queue.get(timeout=timeout)
            logger.debug(
                'Received %s to %s',
                type(message).__name__,
                self.mailbox_id,
            )
            return message
        except QueueClosedError as e:
            raise MailboxClosedError(self.mailbox_id) from e
        
    def clone(self):
        return UnboundThreadExchangeClient(self._state)
