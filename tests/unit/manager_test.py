from __future__ import annotations

import asyncio
import multiprocessing
import pathlib
import time
import uuid
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
from academy.identifier import AgentId
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
async def test_rebind_handle_rekeys_cache(
    manager: Manager[LocalExchangeTransport],
) -> None:
    original_id: AgentId[Any] = AgentId.new()
    new_id: AgentId[Any] = AgentId.new()
    handle = manager.get_handle(original_id)
    assert manager._handles[original_id] is handle

    manager._rebind_handle(handle, new_id)

    assert handle.agent_id == new_id
    assert original_id not in manager._handles
    assert manager._handles[new_id] is handle


@pytest.mark.asyncio
async def test_rebind_handle_same_id_is_noop(
    manager: Manager[LocalExchangeTransport],
) -> None:
    agent_id: AgentId[Any] = AgentId.new()
    handle = manager.get_handle(agent_id)

    manager._rebind_handle(handle, agent_id)

    assert handle.agent_id == agent_id
    assert manager._handles[agent_id] is handle


@pytest.mark.asyncio
async def test_rebind_handle_equal_but_distinct_id_is_noop(
    manager: Manager[LocalExchangeTransport],
) -> None:
    # Same UUID in two AgentId objects exercises the `==` short-circuit
    # rather than `is`.
    shared_uid = uuid.uuid4()
    aid1: AgentId[Any] = AgentId(uid=shared_uid)
    aid2: AgentId[Any] = AgentId(uid=shared_uid)
    assert aid1 == aid2
    assert aid1 is not aid2

    handle = manager.get_handle(aid1)
    manager._rebind_handle(handle, aid2)

    assert handle.agent_id == aid1
    assert manager._handles[aid1] is handle


@pytest.mark.asyncio
async def test_rebind_handle_rejects_conflicting_cached_handle(
    manager: Manager[LocalExchangeTransport],
) -> None:
    aid_a: AgentId[Any] = AgentId.new()
    aid_b: AgentId[Any] = AgentId.new()
    handle_a = manager.get_handle(aid_a)
    manager.get_handle(aid_b)  # cached under aid_b
    with pytest.raises(
        RuntimeError,
        match='already holds a different handle',
    ):
        manager._rebind_handle(handle_a, aid_b)


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
        await batch.launch(EmptyAgent)


@pytest.mark.asyncio
async def test_launch_batch_accepts_instance(
    manager: Manager[LocalExchangeTransport],
) -> None:
    instance = EmptyAgent()
    async with manager.launch_batch() as batch:
        handle = await batch.launch(instance)
        assert batch._intents[0].agent is instance
    assert handle.agent_id in manager.running()


@pytest.mark.asyncio
async def test_launch_batch_queues_and_returns_handle(
    manager: Manager[LocalExchangeTransport],
) -> None:
    async with manager.launch_batch() as batch:
        handle = await batch.launch(EmptyAgent)
        pre_commit_id = handle.agent_id
        assert manager._handles[pre_commit_id] is handle
        assert len(batch._intents) == 1
        intent = batch._intents[0]
        assert intent.agent is EmptyAgent
        assert intent.handle is handle
    # Same handle object is cached under the post-commit id.
    assert manager._handles[handle.agent_id] is handle


@pytest.mark.asyncio
async def test_launch_batch_name_flows_into_agent_id(
    manager: Manager[LocalExchangeTransport],
) -> None:
    async with manager.launch_batch() as batch:
        handle = await batch.launch(EmptyAgent, name='worker-1')
        assert handle.agent_id.name == 'worker-1'
        assert batch._intents[0].name == 'worker-1'


@pytest.mark.asyncio
async def test_launch_batch_multiple_launches_preserve_order(
    manager: Manager[LocalExchangeTransport],
) -> None:
    async with manager.launch_batch() as batch:
        h1 = await batch.launch(EmptyAgent, name='a')
        h2 = await batch.launch(IdentityAgent, name='b')
        h3 = await batch.launch(EmptyAgent, name='c')
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
        parent = await batch.launch(EmptyAgent)
        await batch.launch(
            _FlexAgent,
            args=(parent,),
            kwargs={'peer': parent},
        )
        # Sibling intent captures the parent handle by reference so
        # that commit-time rebinding is visible to siblings.
        sibling_intent = batch._intents[1]
        assert sibling_intent.args is not None
        assert sibling_intent.kwargs is not None
        assert sibling_intent.args[0] is parent
        assert sibling_intent.kwargs['peer'] is parent


@pytest.mark.asyncio
async def test_launch_batch_commits_on_exit(
    manager: Manager[LocalExchangeTransport],
) -> None:
    async with manager.launch_batch() as batch:
        h1 = await batch.launch(EmptyAgent)
        h2 = await batch.launch(IdentityAgent)
    assert len(manager.running()) == 2  # noqa: PLR2004
    assert manager._handles[h1.agent_id] is h1
    assert manager._handles[h2.agent_id] is h2
    running_ids = manager.running()
    assert h1.agent_id in running_ids
    assert h2.agent_id in running_ids


@pytest.mark.asyncio
async def test_launch_batch_commit_rebinds_handle_ids(
    manager: Manager[LocalExchangeTransport],
) -> None:
    # Local has no transport-level ``register_agents``; the client
    # falls back to sequential ``register_agent`` which mints a fresh
    # id. The batch launcher must rebind the pre-minted handle and
    # re-key ``manager._handles``.
    async with manager.launch_batch() as batch:
        handle = await batch.launch(EmptyAgent)
        pre_minted_id = handle.agent_id
        assert manager._handles[pre_minted_id] is handle
    # After commit, the same Handle object is cached under its
    # post-commit id, and the placeholder id is gone from the cache.
    assert manager._handles[handle.agent_id] is handle
    assert pre_minted_id not in manager._handles


@pytest.mark.asyncio
async def test_launch_batch_body_exception_skips_commit(
    manager: Manager[LocalExchangeTransport],
) -> None:
    captured: list[Any] = []

    async def body() -> None:
        async with manager.launch_batch() as batch:
            captured.append(batch)
            await batch.launch(EmptyAgent)
            await batch.launch(IdentityAgent)
            raise ValueError('from body')

    with pytest.raises(ValueError, match='from body'):
        await body()
    assert len(manager.running()) == 0
    # Placeholder handles are evicted so they don't linger pointing at
    # mailboxes that were never created.
    batch = captured[0]
    for intent in batch._intents:
        assert intent.handle.agent_id not in manager._handles


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
async def test_launch_batch_sibling_handle_resolved_after_commit(
    manager: Manager[LocalExchangeTransport],
) -> None:
    captured_sibling_args: list[Any] = []
    async with manager.launch_batch() as batch:
        parent = await batch.launch(EmptyAgent)
        await batch.launch(_FlexAgent, args=(parent,))
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
async def test_launch_batch_register_agents_failure_skips_launches(
    manager: Manager[LocalExchangeTransport],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def failing_register(
        agents: Any,
    ) -> Any:
        raise RuntimeError('injected register_agents failure')

    monkeypatch.setattr(
        manager.exchange_client,
        'register_agents',
        failing_register,
    )

    placeholder_ids: list[Any] = []

    async def body() -> None:
        async with manager.launch_batch() as batch:
            h1 = await batch.launch(EmptyAgent)
            h2 = await batch.launch(EmptyAgent)
            placeholder_ids.extend([h1.agent_id, h2.agent_id])

    with pytest.raises(RuntimeError, match='injected register_agents'):
        await body()

    assert len(manager.running()) == 0
    for pid in placeholder_ids:
        assert pid not in manager._handles


@pytest.mark.asyncio
async def test_launch_batch_launch_failure_terminates_orphans(
    manager: Manager[LocalExchangeTransport],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_launch = manager.launch
    launch_calls: dict[str, int] = {'n': 0}

    async def failing_launch(
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        launch_calls['n'] += 1
        if launch_calls['n'] == 2:  # noqa: PLR2004
            raise RuntimeError('injected launch failure')
        return await original_launch(*args, **kwargs)

    monkeypatch.setattr(manager, 'launch', failing_launch)

    original_terminate = manager.exchange_client.terminate
    terminated: list[Any] = []

    async def wrapped_terminate(agent_id: Any) -> None:
        terminated.append(agent_id)
        await original_terminate(agent_id)

    monkeypatch.setattr(
        manager.exchange_client,
        'terminate',
        wrapped_terminate,
    )

    async def body() -> None:
        async with manager.launch_batch() as batch:
            await batch.launch(EmptyAgent)  # intent 0 — launches
            await batch.launch(EmptyAgent)  # intent 1 — launch fails
            await batch.launch(EmptyAgent)  # intent 2 — orphaned

    with pytest.raises(RuntimeError, match='injected launch failure'):
        await body()

    # Intent 0's agent is still running; intents 1 and 2 were
    # registered but never successfully launched, so they are
    # terminated via exchange_client.terminate.
    assert len(manager.running()) == 1
    assert len(terminated) == 2  # noqa: PLR2004


@pytest.mark.asyncio
async def test_launch_batch_launch_failure_evicts_stale_handles(
    manager: Manager[LocalExchangeTransport],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_launch = manager.launch
    launch_calls: dict[str, int] = {'n': 0}

    async def failing_launch(
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        launch_calls['n'] += 1
        if launch_calls['n'] == 2:  # noqa: PLR2004
            raise RuntimeError('injected launch failure')
        return await original_launch(*args, **kwargs)

    monkeypatch.setattr(manager, 'launch', failing_launch)

    handles: list[Any] = []

    async def body() -> None:
        async with manager.launch_batch() as batch:
            handles.append(await batch.launch(EmptyAgent))
            handles.append(await batch.launch(EmptyAgent))
            handles.append(await batch.launch(EmptyAgent))

    with pytest.raises(RuntimeError, match='injected launch failure'):
        await body()

    running_handle, orphan_rebound, orphan_placeholder = handles
    # Intent 0 launched successfully: handle stays cached.
    assert manager._handles.get(running_handle.agent_id) is running_handle
    # Intent 1 rebound but then the launch failed: evicted under its
    # rebound (real) id.
    assert orphan_rebound.agent_id not in manager._handles
    # Intent 2 never reached the loop: evicted under its placeholder id.
    assert orphan_placeholder.agent_id not in manager._handles


@pytest.mark.asyncio
async def test_launch_batch_orphan_termination_failure_does_not_mask(
    manager: Manager[LocalExchangeTransport],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The original launch exception must surface even when orphan
    # cleanup raises.
    original_launch = manager.launch

    async def failing_launch(
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        if not manager.running():
            return await original_launch(*args, **kwargs)
        raise RuntimeError('injected launch failure')

    monkeypatch.setattr(manager, 'launch', failing_launch)

    async def failing_terminate(agent_id: Any) -> None:
        raise RuntimeError('injected terminate failure')

    monkeypatch.setattr(
        manager.exchange_client,
        'terminate',
        failing_terminate,
    )

    async def body() -> None:
        async with manager.launch_batch() as batch:
            await batch.launch(EmptyAgent)
            await batch.launch(EmptyAgent)

    with pytest.raises(RuntimeError, match='injected launch failure'):
        await body()


@pytest.mark.asyncio
async def test_launch_batch_rejects_rebind_of_used_handle(
    manager: Manager[LocalExchangeTransport],
) -> None:
    async def body() -> None:
        async with manager.launch_batch() as batch:
            handle = await batch.launch(EmptyAgent)
            handle._used_for_messaging = True

    with pytest.raises(RuntimeError, match='used for messaging'):
        await body()


@pytest.mark.asyncio
async def test_launch_batch_mid_commit_cancellation_propagates(
    manager: Manager[LocalExchangeTransport],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Un-launched registrations leak exchange-side on cancellation —
    # documented limitation, not asserted against here.
    original_launch = manager.launch
    first_call = {'seen': False}

    async def launch_then_cancel(*args: Any, **kwargs: Any) -> Any:
        if not first_call['seen']:
            first_call['seen'] = True
            return await original_launch(*args, **kwargs)
        raise asyncio.CancelledError

    monkeypatch.setattr(manager, 'launch', launch_then_cancel)

    async def body() -> None:
        async with manager.launch_batch() as batch:
            await batch.launch(EmptyAgent)
            await batch.launch(EmptyAgent)

    with pytest.raises(asyncio.CancelledError):
        await body()
    assert len(manager.running()) == 1


@pytest.mark.asyncio
async def test_launch_batch_fan_out_sibling_handle(
    manager: Manager[LocalExchangeTransport],
) -> None:
    async with manager.launch_batch() as batch:
        parent = await batch.launch(EmptyAgent)
        await batch.launch(_FlexAgent, args=(parent,))
        await batch.launch(_FlexAgent, kwargs={'peer': parent})
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
        parent_a = await batch.launch(EmptyAgent, name='a')
        parent_b = await batch.launch(EmptyAgent, name='b')
        await batch.launch(
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
