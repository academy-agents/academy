# Concepts

## Architecture

![Architecture](../static/architecture.jpg)
> Agents and users in Academy interact via handles to invoke actions asynchronously.
> An agent's behavior is defined by its actions, control loops, and state.
> Academy decouples the control and data planes through the exchange, used for user and agent communication, and launcher mechanisms that can remotely execute agents.

An Academy application includes one or more *agents* and zero or more *users*.
An agent is a process defined by a *local state*, a set of *actions*, and a set of *control loops*.
Agents can be executed remotely using a *manager* (previously referred to as a *launcher*).
Once running, an agent concurrently executes all of its control loops and listens for messages from user programs or other agents.

A user program interacts with an agent through a *handle*, which acts like a reference to the remote agent and translates method calls into action request messages.
Each entity (i.e., user or agent) has an associated *mailbox* that maintains a queue of messages sent to that entity by other entities.
Mailboxes are maintained by an *exchange* such that any client with access to a given exchange can send messages to the mailbox of another agent in the exchange and receive a response through its own mailbox.

## Agents

In Academy, the concept of an "agent" is intentionally simple. The agent primitive is, at its core, is an entity that:

* **Has state:** Maintains information about its current situation, past history, or internal variables.
* **Performs actions:** Execute specific operations or tasks.
* **Communicates:** Exchanges messages or data with other users, agents, or the environment.

In essence, Academy agents can be thought of as building blocks for more complex or specialized agent-based systems.

An agent is implemented as a Python class that inherits from the base [`Agent`][academy.agent.Agent] type.
This class-based approach is extensible through inheritance and polymorphism.

* **State** is stored as instance attributes on the agent class instance.
Instance attributes maintain the agent's state, and methods define the actions and control loops.
* **Actions** can be performed in two ways: [`@action`][academy.agent.action] decorated methods allow other entities to invoke the method remotely and [`@loop`][academy.agent.loop] decorated methods run non-terminating control loops that enable an agent to autonomously perform actions.
* **Communication** between entities in managed via [`Handles`][academy.handle.Handle] which are client interfaces to remote agents used to invoke actions, ping, and shutdown.

### Execution

The [`Runtime`][academy.runtime.Runtime] manager takes an [`Agent`][academy.agent.Agent] and executes the agent by:
(1) listening for new messages in the agent's mailbox and dispatching them appropriately,
(2) starting each [`@loop`][academy.agent.loop] method,
(3) calling the [`agent_on_startup()`][academy.agent.Agent.agent_on_startup] callback,
and (4) waiting for the agent to be shut down.
Each [`@action`][academy.agent.action] method is executed concurrently in the event loop when requested remotely so as to not block the handling of other messages.

Agents are designed to be long-running, but can be terminated by sending a shutdown request.
Upon shutdown, the shutdown [`Event`][asyncio.Event], passed to each [`@loop`][academy.agent.loop], is set; running tasks are cancelled and waited on; and the [`agent_on_shutdown()`][academy.agent.Agent.agent_on_shutdown] callback is invoked.
Agents can terminate themselves by setting the shutdown event or calling [`Agent.agent_shutdown()`][academy.agent.Agent.agent_shutdown];
exceptions raised in [`@loop`][academy.agent.loop] methods will shutdown the agent by default, and
exceptions raised when executing [`@action`][academy.agent.action] methods are caught and returned to the remote caller.

### Handles

Interacting with an agent is asynchronous; an entity sends a message to the agent's mailbox and waits to receive a response message in its own mailbox.
A [`Handle`][academy.handle.Handle] is a client interface to a remote agent used to invoke actions, ping, and shutdown the agent.
Handles translate method calls into a request messages sent via the exchange and returning a [`Futures`][asyncio.Future].
The handle also listens for response messages and accordingly sets the result on the appropriate [`Futures`][asyncio.Future].

## Exchanges and Mailboxes

Entities communicate by sending and receiving messages to and from mailboxes.
Mailboxes are managed by an exchange, and the [`ExchangeClient`][academy.exchange.ExchangeClient] protocol defines methods for interacting with the exchange and creating handles to other agents.
An [`ExchangeFactory`][academy.exchange.ExchangeFactory] is used to register a new entity with the exchange and create a client that the entity can use for communicating with the exchange.
Registering an entity involves creating a unique ID for the entity, which is also the address of its mailbox, and initializing that mailbox within the exchange.

A mailbox has two states: active and terminated.
Active indicates that the entity's mailbox is accepting messages, even if, for example, an agent has not yet started or is temporarily offline.
Terminated indicates permanent termination of the entity and will cause [`MailboxTerminatedError`][academy.exception.MailboxTerminatedError] to be raised by subsequent send or receive operations to that mailbox.

Academy provides many exchange implementations for different scenarios, such as:

* [**Local**][academy.exchange.local.LocalExchangeFactory]: Uses in-memory queues for single-process, multiple-agent scenarios. Useful for testing and development.
* [**HTTP**][academy.exchange.cloud.client.HttpExchangeFactory]: Centralized service that maintains mailboxes and exposes a REST API. Lower performance but easy to extend with common authentication tools.
* [**Redis**][academy.exchange.redis.RedisExchangeFactory]: Stores state and mailboxes in a Redis server. Use of Redis enables optional replication and cloud-hosting for improved resilience and availability.
* [**HybridExchange**][academy.exchange.hybrid.HybridExchangeFactory]: Entities host their mailbox locally and message each other directly over TCP when possible. Redis is used to map mailbox IDs to address and port pairs, and to store messages for offline entities or when two entities cannot directly communicate (such as when behind NATs).

## Manager

Agents can be run manually via [`Runtime.run_until_complete()`][academy.runtime.Runtime.run_until_complete], but typically applications want to run many agents concurrently across parallel or distributed resources.
The [`Manager`][academy.manager.Manager] provides a single interface for launching and managing agents across one or more [`Executors`][concurrent.futures.Executor], such as a [`ProcessPoolExecutor`][concurrent.futures.ProcessPoolExecutor], [Parsl](https://parsl.readthedocs.io/en/stable/userguide/workflows/workflow.html#parallel-workflows-with-loops){target=_blank}, or [Globus Compute](https://globus-compute.readthedocs.io/en/latest/index.html){target=_blank}.
A manager will handle common boilerplate, including registering agents, creating handles, and ensuring stateful resources are appropriately cleaned up.
