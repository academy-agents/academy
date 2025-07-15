This page documents key breaking changes and tips for migrating between beta versions of Academy.

Please refer to our [Versioning Policy](../contributing/releases.md#versioning) for more details on when we make breaking changes.

## Migrating to v0.3

Academy v0.3 makes a few breaking changes to improve the ergonomics of common usage patterns.

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


## Migrating to v0.2

Academy v0.2 makes numerous breaking changes to nearly every public API.

### Academy is now async

Academy is now an async-first library.
The [asyncio][asyncio] model is better aligned with the highly asynchronous programming model of Academy.
Agent actions and control loops are now executed in the event loop of the main thread, rather than in separate threads.
All exchanges and the manager are async now.

### Renamed Components

Entities are now referred to as agents and users (previously, clients).
Agents are now derived from [`Agent`][academy.agent.Agent] (previously, `Behavior`) and run using a [`Runtime`][academy.runtime.Runtime] (previously, `Agent`).

Summary:

* `academy.agent.Agent` is renamed [`academy.runtime.Runtime`][academy.runtime.Runtime].
* `academy.behavior.Behavior` is renamed [`academy.agent.Agent`][academy.agent.Agent].
* `academy.identifier.ClientId` is renamed [`academy.identifier.UserId`][academy.identifier.UserId].

### Changes to Agents

All special methods provided by [`Agent`][academy.agent.Agent] are named `agent_.*`.
For example, the startup and shutdown callbacks have been renamed:

* `Agent.on_setup` is renamed `Agent.agent_on_startup`
* `Agent.on_shutdown` is renamed `Agent.agent_on_shutdown`

Runtime context is now available via additional methods.

### Changes to Exchanges

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


### Changes to the Manager and Launchers

The `Launcher` protocol and implementations have been removed, with their functionality incorporated directly into the [`Manager`][academy.manager.Manager].

Summary:

* [`Manager`][academy.manager.Manager] is now initialized with one or more [`Executors`][concurrent.futures.Executor].
* Added the [`Manager.from_exchange_factory()`][academy.manager.Manager.from_exchange_factory] class method.
* `Manager.set_default_launcher()` and `Manager.add_launcher()` are renamed [`set_default_executor()`][academy.manager.Manager.set_default_executor] and [`add_executor()`][academy.manager.Manager.add_executor], respectively.
* [`Manager`][academy.manager.Manager] exposes [`get_handle()`][academy.manager.Manager.get_handle] and [`register_agent()`][academy.manager.Manager.register_agent].
* [`Manager.launch()`][academy.manager.Manager.launch] now optionally takes an [`Agent`][academy.agent.Agent] type and args/kwargs and will defer agent initialization to on worker.
* [`Manager.wait()`][academy.manager.Manager] now takes an iterable of agent IDs or handles.
