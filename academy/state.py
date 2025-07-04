from __future__ import annotations

import pathlib
import shelve
from typing import Literal
from typing import TypeVar

DEFAULT_PICKLE_PROTOCOL = 5
ValueT = TypeVar('ValueT')


class FileState(shelve.DbfilenameShelf[ValueT]):
    """Dictionary interface for persistent state.

    Persists arbitrary Python objects to disk using [pickle][pickle] and
    a [dbm][dbm] database.

    Note:
        This class uses the [shelve][shelve] module so refer there for
        additional caveats.

    Example:
        ```python
        from typing import Any
        from academy.agent import Agent, action
        from academy.state import FileState

        class Example(Agent):
            def __init__(self, state_path: str) -> None:
                super().__init__()
                self.state: FileState[Any] = FileState(state_path)

            def agent_on_shutdown(self) -> None:
                self.state.close()

            @action
            def get_state(self, key: str) -> Any:
                return self.state[key]

            @action
            def modify_state(self, key: str, value: Any) -> None:
                self.state[key] = value
        ```

    Note:
        When using the [`Manager`][academy.manager.Manager], use the
        deferred agent initialization to ensure that the state is
        initialized on the worker, rather than the client.

        ```python
        from academy.manager import Manager

        async with Manager(...) as manager:
            handle = manager.launch(Example, args=('/tmp/agent-state.dbm',))
        ```

    Args:
        filename: Base filename for the underlying databased used to store
            key-value pairs.
        flag: Open an existing database read-only: `r`; open an existing
            database for read and write: `w`; open a database for read and
            write, creating it if not existent: `c` (default); always create
            a new empty database for read and write: `n`.
        protocol: Pickling protocol. Defaults to version 5; `None` uses
            the [pickle][pickle] default version.
        writeback: By default (`False`), modified objects are only written
            when assigned. If `True`, the object will hold a cache of all
            entries accessed and write them back to the dict at sync and close
            times. This allows natural operations on mutable entries, but can
            consume much more memory and make sync and close take a long time.
    """

    def __init__(
        self,
        filename: str | pathlib.Path,
        *,
        flag: Literal['r', 'w', 'c', 'n'] = 'c',
        protocol: int | None = DEFAULT_PICKLE_PROTOCOL,
        writeback: bool = False,
    ) -> None:
        super().__init__(
            str(filename),
            flag=flag,
            protocol=protocol,
            writeback=writeback,
        )
