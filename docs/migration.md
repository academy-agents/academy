This guide helps users adapt to breaking changes introduced during the v0.X development cycle, such as:

* Changes to APIs, behavior, or configuration that require user action
* Deprecated or removed features
* Recommended steps for upgrading between versions

We provide migration notes for each v0.X release to make upgrades easier during this early phase of development.
After v1.0, this guide will be retired.
All future changes—including breaking changes and deprecations—will be documented in the project changelog.

Please refer to our [Version Policy](version-policy.md) for more details on when we make breaking changes.

## Academy v0.3

### Handle types have been simplified

The `Handle` protocol and `UnboundRemoteHandle` types have been removed.
`RemoteHandle` has been renamed [`Handle`][academy.handle.Handle] and is the only handle type used in typical Academy applications.
[`ProxyHandle`][academy.handle.ProxyHandle], useful when writing agent unit tests, is now a subclass of [`Handle`][academy.handle.Handle].

### Handles are now free from exchange clients

Previously, handles were bound to a specific [`ExchangeClient`][academy.exchange.ExchangeClient], which was used for sending and receiving messages.
This required handles to be re-bound to the appropriate exchange client based on the context.
Thus, when an agent started it would search the instance attributes of the agent for all handles and replace them at runtime with new handles bound to the agent's exchange client.
This approach resulted in many edge cases where handles would not be discovered and replaced appropriately.

Now, a [`Handle`][academy.handle.Handle] determines the correct [`ExchangeClient`][academy.exchange.ExchangeClient] from [context variables][contextvars].
This exchange context variable is set when running an agent, or on the user-side when using the [`Manager`][academy.manager.Manager] or [`ExchangeClient`][academy.exchange.ExchangeClient] context managers.
As a result, it is now safe to instantiate new handles directly inside an agent using only the ID of the peer agent.
```python
from academy.agent import Agent, action
from academy.handle import Handle

class MyAgent(Agent):
    @action
    async def dispatch(self) -> None:
        handle = Handle(peer_agent_id)
        await handle.do_work()
```

It is highly recommended to use the [`Manager`][academy.manager.Manager] and [`ExchangeClient`][academy.exchange.ExchangeClient] as context managers, otherwise [`Handle`][academy.handle.Handle] operations will raise [`ExchangeClientNotFoundError`][academy.exception.ExchangeClientNotFoundError].
Alternatively, a default exchange client can be explicitly set: `Handle(agent_id, exchange=<exchange_client>)`.
In very specific cases, `ignore_context=True` can be used to force the handle to use the provided exchange client, entirely ignoring the client configured for the current context.

### Handle actions are blocking by default

Previously, invoking an action on a [`Handle`][academy.handle.Handle.action] returned a [`Future`][asyncio.Future] to the result.
This resulted in verbose syntax when the result was immediately needed:
```python
future = await handle.get_count()
result = await future
# or
result = await (await handle.get_count())
```

Now, action requests block and return the final result of the action:
```python
result = await handle.get_count()
```
Code that wants to submit the request and later block on it can create a [`Task`][asyncio.create_task].
```python
task = asyncio.create_task(handle.get_count())
# ... do other work ...
await task
print(task.result())
```
Using tasks is especially useful when launching multiple long-running actions concurrently and waiting for them in a flexible manner.
For example, instead of waiting for each action sequentially, you can start them all at once and then wait for them to complete using [`asyncio.wait()`][asyncio.wait] or [`asyncio.as_completed()`][asyncio.as_completed].

### Exchange implementation re-exports

All of the [`ExchangeFactory`][academy.exchange.factory.ExchangeFactory] and [`ExchangeTransport`][academy.exchange.transport.ExchangeTransport] implementations have been re-exported from [`academy.exchange`][academy.exchange].
[`spawn_http_exchange`][academy.exchange.cloud.client.spawn_http_exchange] has been re-exported from [`academy.exchange.cloud`][academy.exchange.cloud].

## Academy v0.2

### Academy is now async-first

Academy is now an async-first library.
The [asyncio][asyncio] model is better aligned with the highly asynchronous programming model of Academy.
Agent actions and control loops are now executed in the event loop of the main thread, rather than in separate threads.
All exchanges and the manager are async now.

### Renamed components

Entities are now referred to as agents and users (previously, clients).
Agents are now derived from [`Agent`][academy.agent.Agent] (previously, `Behavior`) and run using a [`Runtime`][academy.runtime.Runtime] (previously, `Agent`).

Summary:

* `academy.agent.Agent` is renamed [`academy.runtime.Runtime`][academy.runtime.Runtime].
* `academy.behavior.Behavior` is renamed [`academy.agent.Agent`][academy.agent.Agent].
* `academy.identifier.ClientId` is renamed [`academy.identifier.UserId`][academy.identifier.UserId].

### Changes to agents

All special methods provided by [`Agent`][academy.agent.Agent] are named `agent_.*`.
For example, the startup and shutdown callbacks have been renamed:

* `Agent.on_setup` is renamed `Agent.agent_on_startup`
* `Agent.on_shutdown` is renamed `Agent.agent_on_shutdown`

Runtime context is now available via additional methods.

### Changes to exchanges

The `Exchange` and `Mailbox` protocols have been merged into a single [`ExchangeClient`][academy.exchange.ExchangeClient] which comes in two forms:

* [`AgentExchangeClient`][academy.exchange.AgentExchangeClient]
* [`UserExchangeClient`][academy.exchange.UserExchangeClient]

Thus, an [`ExchangeClient`][academy.exchange.ExchangeClient] has a 1:1 relationship with the mailbox of a single entity.
Each [`ExchangeClient`][academy.exchange.ExchangeClient] is initialized using a [`ExchangeTransport`][academy.exchange.transport.ExchangeTransport].
This protocol defines low-level client interaction with the exchange.
Some of the exchange operations have have been changed:

* `register_client()` has been removed
* [`send()`][academy.exchange.transport.ExchangeTransport.send] no longer takes a `dest` parameter
* [`status()`][academy.exchange.transport.ExchangeTransport.status] has been added

Exchange clients are created using a factory pattern:

* [`ExchangeFactory.create_agent_client()`][academy.exchange.ExchangeFactory.create_agent_client]
* [`ExchangeFactory.create_user_client()`][academy.exchange.ExchangeFactory.create_user_client]

All exchange implementations have been updated to provide a custom transport and factory implementation.
The "thread" exchange has been renamed to "local" now that Academy is async.

All exchange related errors derive from [`ExchangeError`][academy.exception.ExchangeError].
`MailboxClosedError` is renamed [`MailboxTerminatedError`][academy.exception.MailboxTerminatedError] with derived types for [`AgentTerminatedError`][academy.exception.AgentTerminatedError] and [`UserTerminatedError`][academy.exception.UserTerminatedError].


### Changes to the manager and launchers

The `Launcher` protocol and implementations have been removed, with their functionality incorporated directly into the [`Manager`][academy.manager.Manager].

Summary:

* [`Manager`][academy.manager.Manager] is now initialized with one or more [`Executors`][concurrent.futures.Executor].
* Added the [`Manager.from_exchange_factory()`][academy.manager.Manager.from_exchange_factory] class method.
* `Manager.set_default_launcher()` and `Manager.add_launcher()` are renamed [`set_default_executor()`][academy.manager.Manager.set_default_executor] and [`add_executor()`][academy.manager.Manager.add_executor], respectively.
* [`Manager`][academy.manager.Manager] exposes [`get_handle()`][academy.manager.Manager.get_handle] and [`register_agent()`][academy.manager.Manager.register_agent].
* [`Manager.launch()`][academy.manager.Manager.launch] now optionally takes an [`Agent`][academy.agent.Agent] type and args/kwargs and will defer agent initialization to on worker.
* [`Manager.wait()`][academy.manager.Manager] now takes an iterable of agent IDs or handles.
