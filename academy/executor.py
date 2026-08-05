from __future__ import annotations

import asyncio
import threading
from collections.abc import Callable
from concurrent.futures import Executor
from concurrent.futures import Future
from threading import Thread
from typing import Any

import academy.exchange as ae
from academy.agent import action
from academy.agent import Agent
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
        factory: Factory for the same exchange the the manager uses.
    """

    def __init__(
        self,
        factory: ae.ExchangeFactory[Any],
    ):
        self._loop: asyncio.AbstractEventLoop | None = None
        self._loop_ready: threading.Event = threading.Event()
        self._factory = factory
        self._client: ae.UserExchangeClient[Any] | None = None
        self._host: Handle[Any] | None = None
        self._shutdown = False
        self._host_lock: asyncio.Lock = asyncio.Lock()
        self._pending_futures: set[Future[Any]] = set()
        self._host_task: asyncio.Task[None] | None = None

        self._thread: threading.Thread = Thread(target=self._thread_main)
        self._thread.start()
        self._loop_ready.wait()

    def _thread_main(self) -> None:
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._loop_ready.set()
        self._loop.run_forever()
        self._loop.close()

    def submit(
        self,
        fn: Callable[..., Any],
        /,
        *args: Any,
        **kwargs: Any,
    ) -> Future[Any]:
        """Run a callable on the host agent's event loop.

        Args:
            fn: Callable to run on the host.
            *args: Positional arguments for submitted function.
            **kwargs: Keyword arguments for submitted function.

        Returns: Future resolving when fn finishes on host.
        """
        if self._shutdown:
            raise RuntimeError('Cannot submit after host shutdown')

        if fn is _run_agent_on_worker:
            spec = args[0]
            fn, args, kwargs = _run_agent_async, (spec,), {}

        else:
            raise ValueError(
                'Only functions of the type _run_agent_on_worker are allowed to be submitted',  # noqa: E501
            )

        assert self._loop is not None
        task_future = asyncio.run_coroutine_threadsafe(
            self._submit_async(fn, args, kwargs),
            self._loop,
        )
        self._pending_futures.add(task_future)
        task_future.add_done_callback(self._pending_futures.discard)

        return task_future

    async def _submit_async(
        self,
        fn: Callable[..., Any],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> Any:
        if self._host is None:
            async with self._host_lock:
                if self._host is None:  # pragma: no branch
                    await self._launch_host()

        assert self._host is not None

        result = await self._host.submit(fn, args, kwargs)

        return result

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

        self._host_task = asyncio.create_task(_run_agent_async(host_spec))

        self._host = Handle(
            registration.agent_id,
            exchange=self._client,
            ignore_context=True,
        )

    def shutdown(
        self,
        wait: bool = True,
        cancel_futures: bool = True,
    ) -> None:
        """Shut down the owned thread and the host agent.

        Args:
        wait: Wait for the inner executor to finish before returning.
        cancel_futures: Cancel pending futures before shutting down.
        """
        self._shutdown = True
        assert self._loop is not None

        if cancel_futures:
            for future in list(self._pending_futures):
                future.cancel()

        if self._host is not None:
            future_host = asyncio.run_coroutine_threadsafe(
                self._host.shutdown(),
                self._loop,
            )
            future_host.result()

        if self._client is not None:
            future_client = asyncio.run_coroutine_threadsafe(
                self._client.close(),
                self._loop,
            )
            future_client.result()

        self._loop.call_soon_threadsafe(self._loop.stop)

        if wait:
            self._thread.join()
