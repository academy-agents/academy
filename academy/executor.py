from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any
from concurrent.futures import Executor
from concurrent.futures import Future

from academy.agent import action
from academy.agent import Agent
from academy.handle import Handle
from academy.manager import _run_agent_on_worker, _RunSpec
import academy.exchange as ae
from academy.runtime import RuntimeConfig



class _EventLoopHost(Agent):

    """Hidden agent that solely serves to run submitted functions within its own event loop, so that we can share multiple Agents in one worker. 

    """

    @action
    async def submit(
        self,
        fn: Callable[..., Any],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> Any:
        """Run a submitted Callable in this agent's own event loop.
        Args:
            fn: Callable function to run, where an awaitable result is awaited before returning.
            args: Position args for fn.
            kwargs: Keyword args for fn.

        Returns:
        Results of fn and awaited potentially.=
        """

        result = fn(*args, **kwargs)
        if asyncio.iscoroutine(result):
            result = await result
        
        return result
    
class EventLoopExecutor(Executor):
    """Modified Executor that packs muiltiple agents into one event loop of a host agent.

    This executor spends a single inner worker on a hidden host agent, then runs every subsequent Agent submission onto that host's own event loop.

    Args:
        inner: Executor used to run the host agent. 
        factory: Factory for the same exchange the the manager uses. 
    """

    def __init__(self,
                 inner:Executor,
                 factory: ae.ExchangeFactory[Any],
                 ):
        self._inner = inner
        self._factory = factory
        self._client: ae.UserExchangeClient[Any] | None = None
        self._host: Handle [Any] | None = None
        self._host_future: Future[Any] | None = None
        self._shutdown = False

    def submit(self,
               fn: Callable[..., Any],
               /,
               *args:Any,
               **kwargs:Any,
               ) -> Future[Any]:
        
        """Run a callable on the host agent's event loop.

        First call will launch the host onto the inner executor, and subsequent calls will send submissions into the hosts event loop.

        Args: 
            fn: Callable to run on the host.
            Args: Positional args for fn.
            Kwargs: Keyword args for fn.

        Returns: Future resolving when fn finishes on host.
        """

        if self._shutdown:
            raise RuntimeError('Cannot submit after host shutdown')
        
        try: 
            asyncio.get_running_loop()
        except RuntimeError:
            raise RuntimeError(
                "EventloopExecutor submit requires a running event loop to be used for collecting multiple agent runtimes."
            ) from None

        future: Future[Any] = Future()
        asyncio.ensure_future(self._submit_async(fn, args, kwargs, future))
        return future
    
    async def _submit_async(self,
                            fn: Callable[..., Any],
                            args: tuple[Any, ...],
                            kwargs: dict[str, Any],
                            future: Future[Any],) -> None:
        try:
            if self._host is None:
                await self._launch_host()
            if not future.set_running_or_notify_cancel():
                return
            
            result = await self._host.submit(fn, args, kwargs)
        except BaseException as e:
            future.set_exception(e)
        else:
            future.set_result(result)

    async def _launch_host(self) -> None:
        self._client = await self._factory.create_user_client(
            name = 'event-loop-executor'
        )

        registration = await self._client.register_agent(
            _EventLoopHost,
            name = 'event-loop-host'
        )
        host_spec = _RunSpec(
            agent=_EventLoopHost,
            config=RuntimeConfig(),
            exchange_factory = self._factory,
            registration=registration,
            agent_args = (),
            agent_kwargs = {},
            submit_kwargs={},
        )

        self._host_future = self._inner.submit(
            _run_agent_on_worker,
            host_spec
        )

        self._host = Handle(
            registration.agent_id,
            exchange = self._client,
            ignore_context=True
        )

    def shutdown(self,
                 wait: bool=True,
                 *,
                 cancel_futures: bool = False,
                 ) -> None:
        self._shutdown = True,
        self._inner.shutdown(wait=wait, cancel_futures=cancel_futures)


