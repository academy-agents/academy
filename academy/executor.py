from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Callable
from concurrent.futures import Executor
from concurrent.futures import Future
from typing import Any

import academy.exchange as ae
from academy.agent import action
from academy.agent import Agent
from academy.exception import AgentTerminatedError
from academy.handle import Handle
from academy.manager import _run_agent_async
from academy.manager import _run_agent_on_worker
from academy.manager import _RunSpec
from academy.runtime import RuntimeConfig


class _EventLoopHost(Agent):
    """Hidden agent that run submitted functions within own event loop."""

    @action
    async def submit(
        self,
        fn: Callable[..., Any],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> Any:
        """Run a submitted Callable in this agent's own event loop.

        Args:
            fn: Callable function to run, where an awaitable result is awaited
            before returning.
            args: Positional arguments for submitted function.
            kwargs: Keyword arguments for submitted function.


        Returns:
            Results of fn and awaited potentially.
        """
        result = fn(*args, **kwargs)
        if asyncio.iscoroutine(result):
            result = await result

        return result


class EventLoopExecutor(Executor):
    """Modified Executor that packs multiple agents into one event loop.

    This executor spends a single inner worker on a hidden host agent, then
    runs every subsequent Agent submission onto that host's own event loop.

    Args:
        inner: Executor used to run the host agent.
        factory: Factory for the same exchange the the manager uses.
    """

    def __init__(
        self,
        inner: Executor,
        factory: ae.ExchangeFactory[Any],
    ):
        self._inner = inner
        self._factory = factory
        self._client: ae.UserExchangeClient[Any] | None = None
        self._host: Handle[Any] | None = None
        self._host_future: Future[Any] | None = None
        self._shutdown = False
        self._host_lock: asyncio.Lock = asyncio.Lock()
        self._pending_tasks: set[asyncio.Task[None]] = set()

    def submit(
        self,
        fn: Callable[..., Any],
        /,
        *args: Any,
        **kwargs: Any,
    ) -> Future[Any]:
        """Run a callable on the host agent's event loop.

        First call will launch the host onto the inner executor, and subsequent
        calls will send submissions into the hosts event loop.

        Args:
            fn: Callable to run on the host.
            *args: Positional arguments for submitted function.
            **kwargs: Keyword arguments for submitted function.

        Returns: Future resolving when fn finishes on host.
        """
        if self._shutdown:
            raise RuntimeError('Cannot submit after host shutdown')

        try:
            asyncio.get_running_loop()
        except RuntimeError:
            raise RuntimeError(
                'EventloopExecutor submit requires a running event loop to be '
                'used for collecting multiple agent runtimes',
            ) from None

        future: Future[Any] = Future()

        # Since host agent already gives us a event loop, we run the
        # non-wrapper _run_agent_async in the agent instead

        if fn is _run_agent_on_worker:
            spec = args[0]
            fn, args, kwargs = _run_agent_async, (spec,), {}
        task = asyncio.ensure_future(
            self._submit_async(fn, args, kwargs, future),
        )
        self._pending_tasks.add(task)
        task.add_done_callback(self._pending_tasks.discard)
        return future

    async def _submit_async(
        self,
        fn: Callable[..., Any],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        future: Future[Any],
    ) -> None:
        try:
            if self._host is None:
                async with self._host_lock:
                    if self._host is None:  # pragma: no branch
                        await self._launch_host()

            assert self._host is not None

            if not future.set_running_or_notify_cancel():
                return

            result = await self._host.submit(fn, args, kwargs)
        except BaseException as e:
            future.set_exception(e)
        else:
            future.set_result(result)

    async def _launch_host(self) -> None:
        self._client = await self._factory.create_user_client(
            name='event-loop-executor',
        )

        registration = await self._client.register_agent(
            _EventLoopHost,
            name='event-loop-host',
        )
        host_spec = _RunSpec(
            agent=_EventLoopHost,
            config=RuntimeConfig(),
            exchange_factory=self._factory,
            registration=registration,
            agent_args=(),
            agent_kwargs={},
            submit_kwargs={},
        )

        self._host_future = self._inner.submit(
            _run_agent_on_worker,
            host_spec,
        )

        self._host = Handle(
            registration.agent_id,
            exchange=self._client,
            ignore_context=True,
        )

    def shutdown(
        self,
        wait: bool = True,
        *,
        cancel_futures: bool = False,
    ) -> None:
        """Shut down the inner executor.

        Args:
        wait: Wait for the inner executor to finish before returning.
        cancel_futures: Cancel futures in the inner executor.
        """
        self._shutdown = True
        self._inner.shutdown(wait=wait, cancel_futures=cancel_futures)

    async def aclose(self) -> None:
        """Used to shutdown the host agent since it does not exist on acb."""
        self._shutdown = True

        if self._host is not None:
            with contextlib.suppress(AgentTerminatedError):
                await self._host.shutdown()

        if self._host_future is not None:
            await asyncio.wrap_future(self._host_future)

        if self._client is not None:
            await self._client.close()
