from __future__ import annotations

import asyncio
import functools
import inspect
import logging
import sys
import warnings
from collections.abc import Coroutine
from collections.abc import Generator
from datetime import timedelta
from typing import Any
from typing import Callable
from typing import Generic
from typing import Literal
from typing import overload
from typing import Protocol
from typing import TYPE_CHECKING
from typing import TypeVar

if sys.version_info >= (3, 10):  # pragma: >=3.10 cover
    from typing import ParamSpec
    from typing import TypeAlias
else:  # pragma: <3.10 cover
    from typing_extensions import ParamSpec
    from typing_extensions import TypeAlias

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from academy.event import wait_event_async
from academy.exception import AgentNotInitializedError
from academy.handle import Handle
from academy.handle import HandleDict
from academy.handle import HandleList
from academy.handle import ProxyHandle
from academy.handle import RemoteHandle
from academy.handle import UnboundRemoteHandle

if TYPE_CHECKING:
    from academy.context import AgentContext
    from academy.exchange import AgentExchangeClient
    from academy.identifier import AgentId

BehaviorT = TypeVar('BehaviorT', bound='Behavior')
"""Type variable bound to [`Behavior`][academy.behavior.Behavior]."""

P = ParamSpec('P')
R = TypeVar('R')
R_co = TypeVar('R_co', covariant=True)
ActionMethod: TypeAlias = Callable[P, Coroutine[None, None, R]]
LoopMethod: TypeAlias = Callable[
    [BehaviorT, asyncio.Event],
    Coroutine[None, None, None],
]

logger = logging.getLogger(__name__)


class Behavior:
    """Agent behavior base class.

    All [`Agent`][academy.agent.Agent] instances execute a behavior which is
    defined by a subclass of the [`Behavior`][academy.behavior.Behavior]. Each
    behavior is composed of three parts:
      1. The [`on_startup()`][academy.behavior.Behavior.on_setup] and
         [`on_shutdown()`][academy.behavior.Behavior.on_shutdown] methods
         define callbacks that are invoked once at the start and end of an
         agent's execution, respectively. The methods should be used to
         initialize and cleanup stateful resources. Resource initialization
         should not be performed in `__init__`.
      2. Action methods annotated with [`@action`][academy.behavior.action]
         are methods that other agents can invoke on this agent. An agent
         may also call it's own action methods as normal methods.
      3. Control loop methods annotated with [`@loop`][academy.behavior.loop]
         are executed in separate threads when the agent is executed.

    Warning:
        This class cannot be instantiated directly and must be subclassed.
    """

    def __new__(cls, *args: Any, **kwargs: Any) -> Self:  # noqa: D102
        if cls is Behavior:
            raise TypeError(
                f'The {cls.__name__} type cannot be instantiated directly '
                'and must be subclassed.',
            )
        return super().__new__(cls)

    def __init__(self) -> None:
        self.__agent_context: AgentContext[Self] | None = None

    def __repr__(self) -> str:
        return f'{type(self).__name__}()'

    def __str__(self) -> str:
        return f'Behavior<{type(self).__name__}>'

    def _agent_set_context(self, context: AgentContext[Self]) -> None:
        self.__agent_context = context

    @property
    def agent_context(self) -> AgentContext[Self]:
        """Agent runtime context.

        Raises:
            AgentNotInitializedError: If the agent runtime implementing
                this behavior has not been started.
        """
        if (
            # Check _Behavior__agent_context rather than __agent_context
            # because of Python's name mangling
            not hasattr(self, '_Behavior__agent_context')
            or self.__agent_context is None
        ):
            raise AgentNotInitializedError
        return self.__agent_context

    @property
    def agent_id(self) -> AgentId[Self]:
        """Agent Id.

        Raises:
            AgentNotInitializedError: If the agent runtime implementing
                this behavior has not been started.
        """
        return self.agent_context.agent_id

    @property
    def agent_exchange_client(self) -> AgentExchangeClient[Self, Any]:
        """Agent exchange client.

        Raises:
            AgentNotInitializedError: If the agent runtime implementing
                this behavior has not been started.
        """
        return self.agent_context.exchange_client

    def agent_shutdown(self) -> None:
        """Request the agent to shutdown.

        Raises:
            AgentNotInitializedError: If the agent runtime implementing
                this behavior has not been started.
        """
        self.agent_context.shutdown_event.set()

    def _behavior_attributes(self) -> Generator[tuple[str, Any]]:
        for name in dir(self):
            if name in Behavior.__dict__:
                # Skip checking attributes of the base Behavior. Checking
                # the type of properties that access agent_context may
                # raise an AgentNotInitializedError.
                continue
            attr = getattr(self, name)
            yield name, attr

    def behavior_actions(self) -> dict[str, Action[Any, Any]]:
        """Get methods of this behavior type that are decorated as actions.

        Returns:
            Dictionary mapping method names to action methods.
        """
        actions: dict[str, Action[Any, Any]] = {}
        for name, attr in self._behavior_attributes():
            if _is_agent_method_type(attr, 'action'):
                actions[name] = attr
        return actions

    def behavior_loops(self) -> dict[str, ControlLoop]:
        """Get methods of this behavior type that are decorated as loops.

        Returns:
            Dictionary mapping method names to loop methods.
        """
        loops: dict[str, ControlLoop] = {}
        for name, attr in self._behavior_attributes():
            if _is_agent_method_type(attr, 'loop'):
                loops[name] = attr
        return loops

    def behavior_handles(
        self,
    ) -> dict[
        str,
        Handle[Any] | HandleDict[Any, Any] | HandleList[Any],
    ]:
        """Get instance attributes that are agent handles.

        Returns:
            Dictionary mapping attribute names to agent handles or \
            data structures of handles.
        """
        handle_types = (
            ProxyHandle,
            UnboundRemoteHandle,
            RemoteHandle,
            HandleDict,
            HandleList,
        )
        handles: dict[
            str,
            Handle[Any] | HandleDict[Any, Any] | HandleList[Any],
        ] = {}
        for name, attr in self._behavior_attributes():
            if isinstance(attr, handle_types):
                handles[name] = attr
        return handles

    @classmethod
    def behavior_mro(cls) -> tuple[str, ...]:
        """Get the method resolution order of the behavior.

        Example:
            ```python
            >>> from academy.behavior import Behavior
            >>>
            >>> class A(Behavior): ...
            >>> class B(Behavior): ...
            >>> class C(A): ...
            >>> class D(A, B): ...
            >>>
            >>> A.behavior_mro()
            ('__main__.A',)
            >>> B.behavior_mro()
            ('__main__.B',)
            >>> C.behavior_mro()
            ('__main__.C', '__main__.A')
            >>> D.behavior_mro()
            ('__main__.D', '__main__.A', '__main__.B')
            ```

        Returns:
            Tuple of fully-qualified paths of types in the MRO of this \
            behavior type, not including the base \
            [`Behavior`][academy.behavior.Behavior] or [`object`][object].
        """
        mro = cls.mro()
        base_index = mro.index(Behavior)
        mro = mro[:base_index]
        return tuple(f'{t.__module__}.{t.__qualname__}' for t in mro)

    async def on_setup(self) -> None:
        """Callback invoked at the end of an agent's setup sequence.

        See [`Agent.run()`][academy.agent.Agent.run] for more details on the
        setup sequence.
        """
        pass

    async def on_shutdown(self) -> None:
        """Callback invoked at the beginning of an agent's shutdown sequence.

        See [`Agent.run()`][academy.agent.Agent.run] for more details on the
        shutdown sequence.
        """
        pass


class Action(Generic[P, R_co], Protocol):
    """Action method protocol."""

    _agent_method_type: Literal['action']
    _action_method_context: bool

    async def __call__(self, *arg: P.args, **kwargs: P.kwargs) -> R_co:
        """Expected signature of methods decorated as an action.

        In general, action methods can implement any signature.
        """
        ...


class ControlLoop(Protocol):
    """Control loop method protocol."""

    _agent_method_type: Literal['loop']

    async def __call__(self, shutdown: asyncio.Event) -> None:
        """Expected signature of methods decorated as a control loop.

        Args:
            shutdown: Event indicating that the agent has been instructed to
                shutdown and all control loops should exit.

        Returns:
            Control loops should not return anything.
        """
        ...


@functools.lru_cache(maxsize=1)
def _get_handle_protected_methods() -> tuple[str, ...]:
    methods: list[str] = []
    for name, value in inspect.getmembers(Handle):
        # Only include functions defined on Handle, not inherited ones
        if inspect.isfunction(value) and name in Handle.__dict__:
            methods.append(name)
    return tuple(methods)


@overload
def action(method: ActionMethod[P, R]) -> ActionMethod[P, R]: ...


@overload
def action(
    *,
    allow_protected_name: bool = False,
    context: bool = False,
) -> Callable[[ActionMethod[P, R]], ActionMethod[P, R]]: ...


def action(
    method: ActionMethod[P, R] | None = None,
    *,
    allow_protected_name: bool = False,
    context: bool = False,
) -> ActionMethod[P, R] | Callable[[ActionMethod[P, R]], ActionMethod[P, R]]:
    """Decorator that annotates a method of a behavior as an action.

    Marking a method of a behavior as an action makes the method available
    to other agents. I.e., peers within a multi-agent system can only invoke
    methods marked as actions on each other. This enables behaviors to
    define "private" methods.

    Example:
        ```python
        from academy.behavior import Behavior, action
        from academy.context import ActionContext

        class Example(Behavior):
            @action
            async def perform(self) -> ...:
                ...

            @action(context=True)
            async def perform_with_ctx(self, *, context: ActionContext) -> ...:
                ...
        ```

    Warning:
        A warning will be emitted if the decorated method's name clashed
        with a method of [`Handle`][academy.handle.Handle] because it would
        not be possible to invoke this action remotely via attribute
        lookup on a handle. This warning can be suppressed with
        `allow_protected_name=True`, and the action must be invoked via
        [`Handle.action()`][academy.handle.Handle.action].

    Args:
        method: Method to decorate as an action.
        allow_protected_name: Allow decorating a method as an action when
            the name of the method clashes with a protected method name of
            [`Handle`][academy.handle.Handle]. This flag silences the
            emitted warning.
        context: Specify that the action method expects a context argument.
            The `context` will be provided at runtime as a keyword argument.

    Raises:
        TypeError: If `context=True` and the method does not have a parameter
            named `context` or if `context` is a positional only argument.
    """

    def decorator(method_: ActionMethod[P, R]) -> ActionMethod[P, R]:
        if (
            not allow_protected_name
            and method_.__name__ in _get_handle_protected_methods()
        ):
            warnings.warn(
                f'The name of the decorated method is "{method_.__name__}" '
                'which clashes with a protected method of Handle. '
                'Rename the decorated method to avoid ambiguity when remotely '
                'invoking it via a handle.',
                UserWarning,
                stacklevel=3,
            )
        # Typing the requirement that if context=True then params P should
        # contain a keyword argument named "context" is not easily annotated
        # for mypy so instead we check at runtime.
        if context:
            sig = inspect.signature(method_)
            if 'context' not in sig.parameters:
                raise TypeError(
                    f'Action method "{method_.__name__}" must accept a '
                    '"context" keyword argument when used with '
                    '@action(context=True).',
                )
            if (
                sig.parameters['context'].kind
                != inspect.Parameter.KEYWORD_ONLY
            ):
                raise TypeError(
                    'The "context" argument to action method '
                    f'"{method_.__name__}" must be a keyword only argument.',
                )

        method_._agent_method_type = 'action'  # type: ignore[attr-defined]
        method_._action_method_context = context  # type: ignore[attr-defined]
        return method_

    if method is None:
        return decorator
    else:
        return decorator(method)


def loop(method: LoopMethod[BehaviorT]) -> LoopMethod[BehaviorT]:
    """Decorator that annotates a method of a behavior as a control loop.

    Control loop methods of a behavior are run as threads when an agent
    starts. A control loop can run for a well-defined period of time or
    indefinitely, provided the control loop exits when the `shutdown`
    event, passed as a parameter to all control loop methods, is set.

    Example:
        ```python
        import asyncio
        from academy.behavior import Behavior, loop

        class Example(Behavior):
            @loop
            async def listen(self, shutdown: asyncio.Event) -> None:
                while not shutdown.is_set():
                    ...
        ```

    Raises:
        TypeError: if the method signature does not conform to the
            [`ControlLoop`][academy.behavior.ControlLoop] protocol.
    """
    method._agent_method_type = 'loop'  # type: ignore[attr-defined]

    if sys.version_info >= (3, 10):  # pragma: >=3.10 cover
        found_sig = inspect.signature(method, eval_str=True)
        expected_sig = inspect.signature(ControlLoop.__call__, eval_str=True)
    else:  # pragma: <3.10 cover
        found_sig = inspect.signature(method)
        expected_sig = inspect.signature(ControlLoop.__call__)

    if found_sig != expected_sig:
        raise TypeError(
            f'Signature of loop method "{method.__name__}" is {found_sig} '
            f'but should be {expected_sig}. If the signatures look the same '
            'except that types are stringified, try importing '
            '"from __future__ import annotations" at the top of the module '
            'where the behavior is defined.',
        )

    @functools.wraps(method)
    async def _wrapped(self: BehaviorT, shutdown: asyncio.Event) -> None:
        logger.debug('Started %r loop for %s', method.__name__, self)
        await method(self, shutdown)
        logger.debug('Exited %r loop for %s', method.__name__, self)

    return _wrapped


def event(
    name: str,
) -> Callable[
    [Callable[[BehaviorT], Coroutine[None, None, None]]],
    LoopMethod[BehaviorT],
]:
    """Decorator that annotates a method of a behavior as an event loop.

    An event loop is a special type of control loop that runs when a
    [`asyncio.Event`][asyncio.Event] is set. The event is cleared
    after the loop runs.

    Example:
        ```python
        import asyncio
        from academy.behavior import Behavior, timer

        class Example(Behavior):
            def __init__(self) -> None:
                self.alert = asyncio.Event()

            @event('alert')
            async def handle(self) -> None:
                # Runs every time alter is set
                ...
        ```

    Args:
        name: Attribute name of the [`asyncio.Event`][asyncio.Event]
            to wait on.

    Raises:
        AttributeError: Raised at runtime if no attribute named `name`
            exists on the behavior.
        TypeError: Raised at runtime if the attribute named `name` is not
            a [`asyncio.Event`][asyncio.Event].
    """

    def decorator(
        method: Callable[[BehaviorT], Coroutine[None, None, None]],
    ) -> LoopMethod[BehaviorT]:
        method._agent_method_type = 'loop'  # type: ignore[attr-defined]

        @functools.wraps(method)
        async def _wrapped(self: BehaviorT, shutdown: asyncio.Event) -> None:
            event = getattr(self, name)
            if not isinstance(event, asyncio.Event):
                raise TypeError(
                    f'Attribute {name} of {type(self).__class__} has type '
                    f'{type(event).__class__}. Expected threading.Event.',
                )

            logger.debug(
                'Started %r event loop for %s (event: %r)',
                method.__name__,
                self,
                name,
            )
            while not shutdown.is_set():
                await wait_event_async(shutdown, event)
                if event.is_set():
                    try:
                        await method(self)
                    finally:
                        event.clear()
            logger.debug('Exited %r event loop for %s', method.__name__, self)

        return _wrapped

    return decorator


def timer(
    interval: float | timedelta,
) -> Callable[
    [Callable[[BehaviorT], Coroutine[None, None, None]]],
    LoopMethod[BehaviorT],
]:
    """Decorator that annotates a method of a behavior as a timer loop.

    A timer loop is a special type of control loop that runs at a set
    interval. The method will always be called once before the first
    sleep.

    Example:
        ```python
        from academy.behavior import Behavior, timer

        class Example(Behavior):
            @timer(interval=1)
            async def listen(self) -> None:
                # Runs every 1 second
                ...
        ```

    Args:
        interval: Seconds or a [`timedelta`][datetime.timedelta] to wait
            between invoking the method.
    """
    interval = (
        interval.total_seconds()
        if isinstance(interval, timedelta)
        else interval
    )

    def decorator(
        method: Callable[[BehaviorT], Coroutine[None, None, None]],
    ) -> LoopMethod[BehaviorT]:
        method._agent_method_type = 'loop'  # type: ignore[attr-defined]

        @functools.wraps(method)
        async def _wrapped(self: BehaviorT, shutdown: asyncio.Event) -> None:
            logger.debug(
                'Started %r timer loop for %s (interval: %fs)',
                method.__name__,
                self,
                interval,
            )
            while not shutdown.is_set():
                try:
                    await asyncio.wait_for(shutdown.wait(), timeout=interval)
                except asyncio.TimeoutError:
                    await method(self)
            logger.debug('Exited %r timer loop for %s', method.__name__, self)

        return _wrapped

    return decorator


def _is_agent_method_type(obj: Any, kind: str) -> bool:
    return (
        callable(obj)
        and hasattr(obj, '_agent_method_type')
        and obj._agent_method_type == kind
    )
