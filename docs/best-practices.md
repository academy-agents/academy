### Avoid blocking calls in async agent methods

When writing methods on an [`Agent`][academy.agent.Agent] that use `async def`, such as [`@action`][academy.agent.action] and [`@loop`][academy.agent.loop] methods, avoid calling long-running or blocking synchronous functions directly.
Doing so will block the entire asyncio event loop, degrading the responsiveness and concurrency of your agent.

To safely run synchronous (blocking) code inside async methods, use [`Agent.agent_run_sync()`][academy.agent.Agent.agent_run_sync] which runs the function in a separate thread, keeping the event loop free to do other work.
```python
@action
async def do_work(self) -> None:
    result = await self.agent_run_sync(expensive_sync_func)
    ...
```

### Avoid communication operations during agent initialization

The `__init__` method of an [`Agent`][academy.agent.Agent] is called in one of two places:

1. On the client when submitting an agent to be executed.
1. On the worker when agent instantiation is deferred.

In both scenarios, it is unsafe to perform communication operations (i.e., invoking an action on a remote agent) in `__init__` because connection resources and background tasks have not yet been initialized.

The [`Agent.agent_on_startup()`][academy.agent.Agent.agent_on_startup] callback can be used instead to perform communication once the agent is in a running state.

!!! warning

    Be careful when invoking actions on remote agents from the on startup callback.
    An agent will not process incoming action requests until after [`Agent.agent_on_startup()`][academy.agent.Agent.agent_on_startup] has completed.
    This can cause deadlocks when Agent A's startup callback makes a request to Agent B and then Agent B makes a request back to Agent A.
