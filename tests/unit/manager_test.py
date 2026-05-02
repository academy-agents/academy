from __future__ import annotations

import asyncio
import multiprocessing
import pathlib
import pickle
import time
from collections.abc import Callable
from concurrent.futures import Future
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from typing import ParamSpec
from unittest import mock

import pytest

from academy.agent import action
from academy.agent import Agent
from academy.exception import BadEntityIdError
from academy.exception import MailboxTerminatedError
from academy.exception import PingCancelledError
from academy.exchange import HttpExchangeFactory
from academy.exchange import LocalExchangeFactory
from academy.exchange import LocalExchangeTransport
from academy.exchange import UserExchangeClient
from academy.handle import Handle
from academy.logging.configs.file import FileLogging
from academy.manager import Manager
from testing.agents import EmptyAgent
from testing.agents import IdentityAgent
from testing.agents import SleepAgent
from testing.constant import TEST_CONNECTION_TIMEOUT
from testing.constant import TEST_SLEEP_INTERVAL
from testing.constant import TEST_WAIT_TIMEOUT

P = ParamSpec('P')


class _FlexAgent(Agent):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__()


@pytest.mark.asyncio
async def test_from_exchange_factory() -> None:
    async with await Manager.from_exchange_factory(
        factory=LocalExchangeFactory(),
        executors=ThreadPoolExecutor(),
    ) as manager:
        assert isinstance(repr(manager), str)
        assert isinstance(str(manager), str)


@pytest.mark.asyncio
async def test_bad_executor_type() -> None:
    e: Any = ThreadPoolExecutor

    with pytest.raises(ValueError, match='Invalid executors parameter'):
        async with await Manager.from_exchange_factory(
            factory=LocalExchangeFactory(),
            executors=e,
        ):
            ...


@pytest.mark.asyncio
async def test_launch_and_shutdown(
    manager: Manager[LocalExchangeTransport],
) -> None:
    # Two ways to launch: agent instance or deferred agent initialization
    handle1 = await manager.launch(SleepAgent(TEST_SLEEP_INTERVAL))
    handle2 = await manager.launch(SleepAgent, args=(TEST_SLEEP_INTERVAL,))

    assert len(manager.running()) == 2  # noqa: PLR2004

    await asyncio.sleep(5 * TEST_SLEEP_INTERVAL)

    await manager.shutdown(handle1.agent_id)
    await manager.shutdown(handle2)

    await manager.wait({handle1})
    await manager.wait({handle2})

    assert len(manager.running()) == 0

    # Should be a no-op since the agent is already shutdown
    await manager.shutdown(handle1)


@pytest.mark.asyncio
async def test_shutdown_on_exit(
    manager: Manager[LocalExchangeTransport],
) -> None:
    agent = SleepAgent(TEST_SLEEP_INTERVAL)
    # The manager fixture uses a context manager that when exited should
    # shutdown this agent and wait on it
    await manager.launch(agent)
    await asyncio.sleep(5 * TEST_SLEEP_INTERVAL)
    assert len(manager.running()) == 1


@pytest.mark.asyncio
async def test_wait_empty_iterable(
    manager: Manager[LocalExchangeTransport],
) -> None:
    await manager.wait({})


@pytest.mark.asyncio
async def test_wait_bad_identifier(
    manager: Manager[LocalExchangeTransport],
) -> None:
    registration = await manager.register_agent(EmptyAgent)
    with pytest.raises(BadEntityIdError):
        await manager.wait({registration.agent_id})


@pytest.mark.asyncio
async def test_wait_timeout(
    manager: Manager[LocalExchangeTransport],
) -> None:
    agent = SleepAgent(TEST_SLEEP_INTERVAL)
    handle = await manager.launch(agent)
    with pytest.raises(TimeoutError):
        await manager.wait({handle}, timeout=TEST_SLEEP_INTERVAL)


@pytest.mark.asyncio
async def test_wait_timeout_all_completed(
    manager: Manager[LocalExchangeTransport],
) -> None:
    agent = SleepAgent(TEST_SLEEP_INTERVAL)
    handle1 = await manager.launch(agent)
    handle2 = await manager.launch(agent)
    await manager.shutdown(handle1, blocking=True)
    with pytest.raises(TimeoutError):
        await manager.wait(
            {handle1, handle2},
            timeout=TEST_SLEEP_INTERVAL,
            return_when=asyncio.ALL_COMPLETED,
        )


@pytest.mark.asyncio
async def test_shutdown_bad_identifier(
    manager: Manager[LocalExchangeTransport],
) -> None:
    registration = await manager.register_agent(EmptyAgent)
    with pytest.raises(BadEntityIdError):
        await manager.shutdown(registration.agent_id)


@pytest.mark.asyncio
async def test_register_agents_and_launch(
    manager: Manager[LocalExchangeTransport],
) -> None:
    registrations = await manager.register_agents(
        [(EmptyAgent, None), (IdentityAgent, None)],
    )
    assert len(registrations) == 2  # noqa: PLR2004
    await manager.launch(
        EmptyAgent(),
        registration=registrations[0],
    )
    await manager.launch(
        IdentityAgent(),
        registration=registrations[1],
    )
    assert len(manager.running()) == 2  # noqa: PLR2004


@pytest.mark.asyncio
async def test_launch_batch_empty_is_noop(
    manager: Manager[LocalExchangeTransport],
) -> None:
    async with manager.launch_batch():
        pass
    assert len(manager.running()) == 0


@pytest.mark.asyncio
async def test_launch_batch_launch_after_close_raises(
    manager: Manager[LocalExchangeTransport],
) -> None:
    batch = manager.launch_batch()
    async with batch:
        pass
    with pytest.raises(RuntimeError, match='batch is closed'):
        await batch.queue(EmptyAgent)


@pytest.mark.asyncio
async def test_launch_batch_accepts_instance(
    manager: Manager[LocalExchangeTransport],
) -> None:
    instance = EmptyAgent()
    async with manager.launch_batch() as batch:
        handle = await batch.queue(instance)
        assert batch._intents[0].agent is instance
    assert handle.agent_id in manager.running()


@pytest.mark.asyncio
async def test_launch_batch_queues_and_returns_handle(
    manager: Manager[LocalExchangeTransport],
) -> None:
    async with manager.launch_batch() as batch:
        handle = await batch.queue(EmptyAgent)
        with pytest.raises(RuntimeError, match='not bound'):
            _ = handle.agent_id
        assert len(batch._intents) == 1
        intent = batch._intents[0]
        assert intent.agent is EmptyAgent
        assert intent.handle is handle
    assert manager._handles[handle.agent_id] is handle


@pytest.mark.asyncio
async def test_launch_batch_name_flows_into_agent_id(
    manager: Manager[LocalExchangeTransport],
) -> None:
    async with manager.launch_batch() as batch:
        handle = await batch.queue(EmptyAgent, name='worker-1')
        assert batch._intents[0].name == 'worker-1'
    assert handle.agent_id.name == 'worker-1'


@pytest.mark.asyncio
async def test_launch_batch_multiple_launches_preserve_order(
    manager: Manager[LocalExchangeTransport],
) -> None:
    async with manager.launch_batch() as batch:
        h1 = await batch.queue(EmptyAgent, name='a')
        h2 = await batch.queue(IdentityAgent, name='b')
        h3 = await batch.queue(EmptyAgent, name='c')
        assert batch._intents[0].agent is EmptyAgent
        assert batch._intents[1].agent is IdentityAgent
        assert batch._intents[2].agent is EmptyAgent
        assert batch._intents[0].name == 'a'
        assert batch._intents[1].name == 'b'
        assert batch._intents[2].name == 'c'
    ids = {h1.agent_id, h2.agent_id, h3.agent_id}
    assert len(ids) == 3  # noqa: PLR2004


@pytest.mark.asyncio
async def test_launch_batch_handle_usable_as_sibling_arg(
    manager: Manager[LocalExchangeTransport],
) -> None:
    async with manager.launch_batch() as batch:
        parent = await batch.queue(EmptyAgent)
        await batch.queue(
            _FlexAgent,
            args=(parent,),
            kwargs={'peer': parent},
        )
        # Sibling intent captures the parent handle by reference so
        # that submit-time rebinding is visible to siblings.
        sibling_intent = batch._intents[1]
        assert sibling_intent.args is not None
        assert sibling_intent.kwargs is not None
        assert sibling_intent.args[0] is parent
        assert sibling_intent.kwargs['peer'] is parent


@pytest.mark.asyncio
async def test_launch_batch_submits_on_exit(
    manager: Manager[LocalExchangeTransport],
) -> None:
    async with manager.launch_batch() as batch:
        h1 = await batch.queue(EmptyAgent)
        h2 = await batch.queue(IdentityAgent)
    assert len(manager.running()) == 2  # noqa: PLR2004
    assert manager._handles[h1.agent_id] is h1
    assert manager._handles[h2.agent_id] is h2
    running_ids = manager.running()
    assert h1.agent_id in running_ids
    assert h2.agent_id in running_ids


@pytest.mark.asyncio
async def test_launch_batch_submit_binds_handle(
    manager: Manager[LocalExchangeTransport],
) -> None:
    # Pre-submit the handle is unbound. Submit registers an agent
    # and binds the handle to the transport-minted id.
    async with manager.launch_batch() as batch:
        handle = await batch.queue(EmptyAgent)
        assert handle._agent_id is None
    assert manager._handles[handle.agent_id] is handle


@pytest.mark.asyncio
async def test_launch_batch_body_exception_skips_submit(
    manager: Manager[LocalExchangeTransport],
) -> None:
    handles: list[Any] = []

    async def body() -> None:
        async with manager.launch_batch() as batch:
            handles.append(await batch.queue(EmptyAgent))
            handles.append(await batch.queue(IdentityAgent))
            raise ValueError('from body')

    with pytest.raises(ValueError, match='from body'):
        await body()
    assert len(manager.running()) == 0
    # Body raised before submit; handles were never bound.
    for handle in handles:
        assert handle._agent_id is None


@pytest.mark.asyncio
async def test_launch_batch_empty_body_does_not_call_register_agents(
    manager: Manager[LocalExchangeTransport],
) -> None:
    with mock.patch.object(
        manager.exchange_client,
        'register_agents',
        new_callable=mock.AsyncMock,
    ) as register_agents:
        async with manager.launch_batch():
            pass
    assert register_agents.call_count == 0


@pytest.mark.asyncio
async def test_launch_batch_sibling_handle_resolved_after_submit(
    manager: Manager[LocalExchangeTransport],
) -> None:
    captured_sibling_args: list[Any] = []
    async with manager.launch_batch() as batch:
        parent = await batch.queue(EmptyAgent)
        await batch.queue(_FlexAgent, args=(parent,))
        sibling_args = batch._intents[1].args
        assert sibling_args is not None
        captured_sibling_args.append(sibling_args)
    assert len(manager.running()) == 2  # noqa: PLR2004
    # The captured parent handle has been mutated in-place to carry
    # the transport-minted id, so a worker that pickles it now will
    # send messages to the running parent.
    sibling_parent_ref = captured_sibling_args[0][0]
    assert sibling_parent_ref is parent
    assert sibling_parent_ref.agent_id in manager.running()


@pytest.mark.asyncio
async def test_launch_batch_evicts_on_register_failure(
    manager: Manager[LocalExchangeTransport],
) -> None:
    failing_register = mock.AsyncMock(
        side_effect=RuntimeError('injected register_agents failure'),
    )
    handles: list[Any] = []

    async def body() -> None:
        async with manager.launch_batch() as batch:
            handles.append(await batch.queue(EmptyAgent))
            handles.append(await batch.queue(EmptyAgent))

    with (
        mock.patch.object(
            manager.exchange_client,
            'register_agents',
            new=failing_register,
        ),
        pytest.raises(RuntimeError, match='injected register_agents failure'),
    ):
        await body()

    failing_register.assert_awaited_once()
    assert len(manager.running()) == 0
    # Register failed before any rebind happened, so the handles
    # remain unbound and were never inserted into the cache.
    for handle in handles:
        assert handle._agent_id is None


@pytest.mark.asyncio
async def test_launch_batch_terminates_on_launch_failure(
    manager: Manager[LocalExchangeTransport],
) -> None:
    original_launch = manager.launch
    launch_calls: dict[str, int] = {'n': 0}

    async def _failing_launch(*args: Any, **kwargs: Any) -> Any:
        launch_calls['n'] += 1
        if launch_calls['n'] == 2:  # noqa: PLR2004
            raise RuntimeError('injected launch failure')
        return await original_launch(*args, **kwargs)

    failing_launch = mock.AsyncMock(side_effect=_failing_launch)
    wrapped_terminate = mock.AsyncMock(
        wraps=manager.exchange_client.terminate,
    )

    async def body() -> None:
        async with manager.launch_batch() as batch:
            await batch.queue(EmptyAgent)  # Launches
            await batch.queue(EmptyAgent)  # Fails to launch
            await batch.queue(EmptyAgent)  # Orphaned

    with (
        mock.patch.object(manager, 'launch', new=failing_launch),
        mock.patch.object(
            manager.exchange_client,
            'terminate',
            new=wrapped_terminate,
        ),
        pytest.raises(RuntimeError, match='injected launch failure'),
    ):
        await body()

    # First launch succeeds
    # Second launch fails to launch
    # Third launch is orphaned
    assert failing_launch.await_count == 2  # noqa: PLR2004
    assert wrapped_terminate.await_count == 2  # noqa: PLR2004
    assert len(manager.running()) == 1


@pytest.mark.asyncio
async def test_launch_batch_evicts_on_launch_failure(
    manager: Manager[LocalExchangeTransport],
) -> None:
    original_launch = manager.launch
    launch_calls: dict[str, int] = {'n': 0}

    async def _failing_launch(*args: Any, **kwargs: Any) -> Any:
        launch_calls['n'] += 1
        if launch_calls['n'] == 2:  # noqa: PLR2004
            raise RuntimeError('injected launch failure')
        return await original_launch(*args, **kwargs)

    failing_launch = mock.AsyncMock(side_effect=_failing_launch)

    handles: list[Any] = []

    async def body() -> None:
        async with manager.launch_batch() as batch:
            handles.append(await batch.queue(EmptyAgent))  # Launches
            handles.append(await batch.queue(EmptyAgent))  # Fails to launch
            handles.append(await batch.queue(EmptyAgent))  # Orphaned

    with (
        mock.patch.object(manager, 'launch', new=failing_launch),
        pytest.raises(RuntimeError, match='injected launch failure'),
    ):
        await body()

    assert failing_launch.await_count == 2  # noqa: PLR2004
    running, failed, orphaned = handles
    # First launch succeeded: bound and cached.
    assert manager._handles.get(running.agent_id) is running
    # Second launch's handle was bound (agent_id was set) but the
    # launch call failed; the cache entry was popped during rollback.
    assert failed._agent_id is not None
    assert failed._agent_id not in manager._handles
    # Third launch was never reached: the handle is still unbound.
    assert orphaned._agent_id is None


@pytest.mark.asyncio
async def test_launch_batch_orphan_termination_failure_does_not_mask(
    manager: Manager[LocalExchangeTransport],
) -> None:
    # The original launch exception must surface even when orphan
    # cleanup raises.
    original_launch = manager.launch

    async def _failing_launch(*args: Any, **kwargs: Any) -> Any:
        if not manager.running():
            return await original_launch(*args, **kwargs)
        raise RuntimeError('injected launch failure')

    failing_launch = mock.AsyncMock(side_effect=_failing_launch)
    failing_terminate = mock.AsyncMock(
        side_effect=RuntimeError('injected terminate failure'),
    )

    async def body() -> None:
        async with manager.launch_batch() as batch:
            await batch.queue(EmptyAgent)
            await batch.queue(EmptyAgent)

    with (
        mock.patch.object(manager, 'launch', new=failing_launch),
        mock.patch.object(
            manager.exchange_client,
            'terminate',
            new=failing_terminate,
        ),
        pytest.raises(RuntimeError, match='injected launch failure'),
    ):
        await body()

    # intent to launch first agent succeeds
    # intent to launch second agent fails to launch
    assert failing_launch.await_count == 2  # noqa: PLR2004
    # Rollback is run once for the orphaned agent left by
    # the failing launch.
    assert failing_terminate.await_count == 1


@pytest.mark.asyncio
async def test_launch_batch_propagates_cancellation(
    manager: Manager[LocalExchangeTransport],
) -> None:
    # Un-launched registrations leak exchange-side on cancellation.
    # This is expected & not asserted against here.
    original_launch = manager.launch
    first_call = {'seen': False}

    async def _launch_then_cancel(*args: Any, **kwargs: Any) -> Any:
        if not first_call['seen']:
            first_call['seen'] = True
            return await original_launch(*args, **kwargs)
        raise asyncio.CancelledError

    launch_then_cancel = mock.AsyncMock(side_effect=_launch_then_cancel)

    async def body() -> None:
        async with manager.launch_batch() as batch:
            await batch.queue(EmptyAgent)
            await batch.queue(EmptyAgent)

    with (
        mock.patch.object(manager, 'launch', new=launch_then_cancel),
        pytest.raises(asyncio.CancelledError),
    ):
        await body()

    # The first launch intent launched. Second intent was
    # cancelled before it could be launched.
    assert launch_then_cancel.await_count == 2  # noqa: PLR2004


@pytest.mark.asyncio
async def test_launch_batch_explicit_submit(
    manager: Manager[LocalExchangeTransport],
) -> None:
    batch = manager.launch_batch()
    h1 = await batch.queue(EmptyAgent)
    h2 = await batch.queue(EmptyAgent)
    await batch.submit()

    assert h1.agent_id in manager.running()
    assert h2.agent_id in manager.running()


@pytest.mark.asyncio
async def test_launch_batch_abort_discards_intents(
    manager: Manager[LocalExchangeTransport],
) -> None:
    batch = manager.launch_batch()
    h1 = await batch.queue(EmptyAgent)
    h2 = await batch.queue(EmptyAgent)
    batch.abort()

    assert len(manager.running()) == 0
    assert h1._agent_id is None
    assert h2._agent_id is None


@pytest.mark.asyncio
async def test_launch_batch_submit_after_close_raises(
    manager: Manager[LocalExchangeTransport],
) -> None:
    batch = manager.launch_batch()
    await batch.queue(EmptyAgent)
    await batch.submit()
    with pytest.raises(RuntimeError, match='batch is closed'):
        await batch.submit()


@pytest.mark.asyncio
async def test_launch_batch_aexit_skips_after_explicit_submit(
    manager: Manager[LocalExchangeTransport],
) -> None:
    async with manager.launch_batch() as batch:
        handle = await batch.queue(EmptyAgent)
        await batch.submit()
    assert manager._handles[handle.agent_id] is handle


@pytest.mark.asyncio
async def test_launch_batch_abort_is_idempotent(
    manager: Manager[LocalExchangeTransport],
) -> None:
    batch = manager.launch_batch()
    await batch.queue(EmptyAgent)
    batch.abort()
    batch.abort()


def test_handle_pickle_unbound_raises() -> None:
    handle: Handle[Any] = Handle()
    with pytest.raises(pickle.PicklingError, match='unbound'):
        pickle.dumps(handle)


@pytest.mark.asyncio
async def test_launch_batch_fan_out_sibling_handle(
    manager: Manager[LocalExchangeTransport],
) -> None:
    async with manager.launch_batch() as batch:
        parent = await batch.queue(EmptyAgent)
        await batch.queue(_FlexAgent, args=(parent,))
        await batch.queue(_FlexAgent, kwargs={'peer': parent})
    running = manager.running()
    assert len(running) == 3  # noqa: PLR2004
    assert parent.agent_id in running
    assert manager._handles[parent.agent_id] is parent


@pytest.mark.asyncio
async def test_launch_batch_diamond_sibling_handles(
    manager: Manager[LocalExchangeTransport],
) -> None:
    captured: list[Any] = []
    async with manager.launch_batch() as batch:
        parent_a = await batch.queue(EmptyAgent, name='a')
        parent_b = await batch.queue(EmptyAgent, name='b')
        await batch.queue(
            _FlexAgent,
            args=(parent_a, parent_b),
        )
        captured.append(batch._intents[2].args)
    running = manager.running()
    assert len(running) == 3  # noqa: PLR2004
    assert parent_a.agent_id in running
    assert parent_b.agent_id in running
    child_args = captured[0]
    assert child_args[0] is parent_a
    assert child_args[1] is parent_b


@pytest.mark.asyncio
async def test_duplicate_launched_agents_error(
    manager: Manager[LocalExchangeTransport],
) -> None:
    registration = await manager.register_agent(EmptyAgent)
    await manager.launch(EmptyAgent(), registration=registration)
    with pytest.raises(
        RuntimeError,
        match=f'{registration.agent_id} has already been executed.',
    ):
        await manager.launch(EmptyAgent(), registration=registration)
    assert len(manager.running()) == 1


@pytest.mark.asyncio
async def test_shutdown_nonblocking(
    manager: Manager[LocalExchangeTransport],
) -> None:
    agent = SleepAgent(TEST_SLEEP_INTERVAL)
    handle = await manager.launch(agent)
    await manager.shutdown(handle, blocking=False)
    await manager.wait({handle}, timeout=TEST_WAIT_TIMEOUT)


@pytest.mark.asyncio
async def test_shutdown_blocking(
    manager: Manager[LocalExchangeTransport],
) -> None:
    agent = SleepAgent(TEST_SLEEP_INTERVAL)
    handle = await manager.launch(agent)
    await manager.shutdown(handle, blocking=True)
    assert len(manager.running()) == 0


@pytest.mark.asyncio
async def test_bad_default_executor(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
) -> None:
    with pytest.raises(ValueError, match='No executor named "second"'):
        Manager(
            exchange_client=exchange_client,
            executors={'first': ThreadPoolExecutor()},
            default_executor='second',
        )


@pytest.mark.asyncio
async def test_add_and_set_executor_errors(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
) -> None:
    executor = ThreadPoolExecutor()
    async with Manager(
        exchange_client=exchange_client,
        executors={'first': executor},
    ) as manager:
        with pytest.raises(
            ValueError,
            match=r'Executor named "first" already exists\.',
        ):
            manager.add_executor('first', executor)
        with pytest.raises(
            ValueError,
            match=r'An executor named "second" does not exist\.',
        ):
            manager.set_default_executor('second')


@pytest.mark.asyncio
async def test_multiple_executor(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
) -> None:
    async with Manager(
        exchange_client=exchange_client,
        executors={'first': ThreadPoolExecutor()},
    ) as manager:
        await manager.launch(EmptyAgent(), executor='first')

        manager.add_executor('second', ThreadPoolExecutor())
        manager.set_default_executor('second')
        await manager.launch(EmptyAgent())
        await manager.launch(EmptyAgent(), executor='first')


class FailOnStartupAgent(Agent):
    def __init__(self, max_errors: int | None = None) -> None:
        self.errors = 0
        self.max_errors = max_errors

    async def agent_on_startup(self) -> None:
        if self.max_errors is None or self.errors < self.max_errors:
            await asyncio.sleep(1)
            self.errors += 1
            raise RuntimeError('Agent startup failed')


@pytest.mark.asyncio
async def test_ping_cancelled_error(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
) -> None:
    # Note: this test presently relies on agent to be shared across
    # each agent execution in other threads.
    agent = FailOnStartupAgent(max_errors=1)
    async with Manager(
        exchange_client,
        max_retries=2,
    ) as manager:
        handle = await manager.launch(agent)
        with pytest.raises(PingCancelledError):
            await handle.ping(timeout=TEST_CONNECTION_TIMEOUT)


@pytest.mark.asyncio
async def test_retry_on_error(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
) -> None:
    # Note: this test presently relies on agent to be shared across
    # each agent execution in other threads.
    agent = FailOnStartupAgent(max_errors=2)
    async with Manager(
        exchange_client,
        max_retries=3,
    ) as manager:
        handle = await manager.launch(agent)
        while True:
            try:
                await handle.ping(timeout=TEST_CONNECTION_TIMEOUT)
            except PingCancelledError:
                pass
            else:
                break

        await handle.ping(timeout=TEST_CONNECTION_TIMEOUT)

        # assert agent.errors == 2
        await handle.shutdown()


@pytest.mark.parametrize('raise_error', (True, False))
@pytest.mark.asyncio
async def test_wait_ignore_agent_errors(
    raise_error: bool,
    exchange_client: UserExchangeClient[LocalExchangeTransport],
) -> None:
    agent = FailOnStartupAgent()
    manager = Manager(
        exchange_client,
        executors=ThreadPoolExecutor(max_workers=1),
    )
    handle = await manager.launch(agent)

    if raise_error:
        with pytest.raises(RuntimeError, match='Agent startup failed'):
            await manager.wait({handle}, raise_error=raise_error)
    else:
        await manager.wait({handle}, raise_error=raise_error)

    with pytest.raises(RuntimeError, match='Agent startup failed'):
        await manager.close()


@pytest.mark.asyncio
async def test_warn_executor_overload(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
) -> None:
    agent = SleepAgent(TEST_SLEEP_INTERVAL)
    async with Manager(
        exchange_client,
        executors=ThreadPoolExecutor(max_workers=1),
    ) as manager:
        await manager.launch(agent)
        with pytest.warns(RuntimeWarning, match='Executor overload:'):
            await manager.launch(agent)
        assert len(manager.running()) == 2  # noqa: PLR2004


@pytest.mark.asyncio
async def test_executor_pass_kwargs(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
) -> None:
    class MockExecutor(ThreadPoolExecutor):
        def submit(
            self,
            fn: Callable[P, Any],
            /,
            *args: P.args,
            **kwargs: P.kwargs,
        ) -> Future[Any]:
            assert 'parsl_resource_spec' in kwargs
            return super().submit(fn, *args, **kwargs)

    agent = SleepAgent(TEST_SLEEP_INTERVAL)
    async with Manager(
        exchange_client,
        executors=MockExecutor(),
    ) as manager:
        hdl = await manager.launch(
            agent,
            submit_kwargs={'parsl_resource_spec': {'cores': 1}},
        )
        await hdl.ping(timeout=TEST_WAIT_TIMEOUT)
        await hdl.shutdown()


# Logging done in a subprocess is not captured by pytest so we cannot use
# pytest's caplog fixture to validate output.
@pytest.mark.asyncio
async def test_worker_init_logging_logfile(
    http_exchange_factory: HttpExchangeFactory,
    tmp_path: pathlib.Path,
) -> None:
    filepath = tmp_path / 'test-worker-init-logging.log'

    lc = FileLogging(logfile=filepath)
    spawn_context = multiprocessing.get_context('spawn')

    async with await Manager.from_exchange_factory(
        http_exchange_factory,
        executors=ProcessPoolExecutor(max_workers=1, mp_context=spawn_context),
    ) as manager:
        agent = SleepAgent(TEST_SLEEP_INTERVAL)
        handle = await manager.launch(
            agent,
            log_config=lc,
        )
        await handle.shutdown()
        await manager.wait({handle})

    assert filepath.exists(), 'log file from manager should exist'


@pytest.mark.asyncio
async def test_event_loop_agent_launch(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
):
    agent = IdentityAgent()
    async with await Manager.from_exchange_factory(
        factory=exchange_client.factory(),
    ) as manager:
        hdl = await manager.launch(agent)
        result = await hdl.identity('hello')
        assert result == 'hello'

        second = await manager.launch(agent, executor='event_loop')

        await hdl.shutdown()
        await second.shutdown()
        await manager.wait([hdl, second])


@pytest.mark.asyncio
async def test_terminate_mailbox_on_launch_error(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
):
    def delay_exception():
        time.sleep(TEST_SLEEP_INTERVAL)
        raise RuntimeError()

    class FakeExecutor(ThreadPoolExecutor):
        def submit(
            self,
            fn: Callable[P, Any],
            /,
            *args: P.args,
            **kwargs: P.kwargs,
        ) -> Future[Any]:
            return super().submit(delay_exception)

    class TestAgent(Agent):
        @action
        async def echo(self, thing: Any) -> Any:  # pragma: no cover
            return 'Hello'

    # Do not want to use context manager so we can explicitly expect
    # a RuntimeError while closing the manager
    manager = Manager(exchange_client, executors=FakeExecutor())

    hdl = await manager.launch(TestAgent)
    with pytest.raises(MailboxTerminatedError):
        await hdl.echo('Hello')

    with pytest.raises(RuntimeError):
        await manager.close()
