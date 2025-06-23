from __future__ import annotations

import threading
from typing import Any
from typing import Callable


class ThreadWithErrorHandling:
    """Run a callable in a thread with extra error handling.

    Args:
        target: Callable to run in the thread.
        name: Thread name.
        args: Positional arguments to pass to the callable.
        kwargs: Keyword arguments to pass to the callable.
    """

    def __init__(
        self,
        target: Callable[..., Any],
        *,
        name: str | None = None,
        args: tuple[Any, ...] = (),
        kwargs: dict[str, Any] | None = None,
    ) -> None:
        kwargs = kwargs if kwargs is not None else {}
        self._exception: Exception | None = None

        def _exec() -> None:
            try:
                target(*args, **kwargs)
            except Exception as e:
                self._exception = e

        self._thread = threading.Thread(target=_exec, name=name)
        self._thread.start()

    def join(self, timeout: float | None = None) -> None:
        """Wait until the thread terminates.

        Args:
            timeout: Timeout in seconds to block for.

        Raises:
            TimeoutError: If the thread did not join within `timeout` seconds.
            Exception: Any exception raised in the thread.
        """
        self._thread.join(timeout)
        if self._thread.is_alive():
            raise TimeoutError('Thread join timed out.')
        if self._exception is not None:
            raise self._exception
