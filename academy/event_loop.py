from __future__ import annotations

import asyncio
import sys
import threading
from collections.abc import Coroutine
from concurrent.futures import Future
from types import TracebackType
from typing import TypeVar

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from academy.thread import ThreadWithErrorHandling

T = TypeVar('T')


class EventLoopRunner:
    """Run an event loop in a separate thread.

    ```python
    import asyncio
    from academy.event_loop import EventLoopRunner

    async def add(x: int, y: int) -> int:
        await asyncio.sleep(0.1)
        return x + y

    with EventLoopRunner() as runner:
        future = runner.run_coroutine(add(3, 4))
        print('Result: {future.result()}')  # Result: 7
    ```

    Args:
        loop: Event loop to run events in. By default a new event loop is
            created. If a loop is provided and is already running, a new
            thread will not be started.
        timeout: Timeout in seconds to wait for the event loop to start and
            stop.

    Raises:
        TimeoutError: If the event loop is not running within `timeout`
            seconds.
    """

    def __init__(
        self,
        *,
        loop: asyncio.AbstractEventLoop | None = None,
        timeout: float | None = None,
    ) -> None:
        self._timeout = timeout
        self._loop = asyncio.new_event_loop() if loop is None else loop

        self._thread: ThreadWithErrorHandling | None = None
        if not self._loop.is_running():
            loop_ready = threading.Event()

            def _run_loop() -> None:
                asyncio.set_event_loop(self._loop)
                loop_ready.set()
                self._loop.run_forever()
                self._loop.close()

            self._thread = ThreadWithErrorHandling(
                target=_run_loop,
                name='event-loop-runner-thread',
            )
            loop_ready.wait(timeout=self._timeout)

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        self.stop()

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        """Event loop."""
        return self._loop

    def run_coroutine(
        self,
        coro: Coroutine[None, None, T],
    ) -> Future[T]:
        """Submit a coroutine to be run in the loop.

        Args:
            coro: Coroutine to be run.

        Returns:
            Future that will contain the result of the coroutine.

        Raises:
            RuntimeError: If the loop has been stopped.
        """
        if self._loop.is_closed():
            raise RuntimeError('Loop has been stopped.')
        return asyncio.run_coroutine_threadsafe(coro, self._loop)

    def stop(self) -> None:
        """Join the event loop thread.

        Instructs the event loop to close and waits on the event loop thread
        to exit. If an already running event loop was provided, this method
        does nothing.

        Raises:
            TimeoutError: If the event loop thread does join exit with
                `timeout` seconds as specified in the constructor.
        """
        if self._loop.is_closed():
            return

        self._loop.call_soon_threadsafe(self._loop.stop)
        if self._thread is not None:
            self._thread.join(self._timeout)
