from __future__ import annotations

import sys
from collections.abc import Coroutine

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import assert_type
else:  # pragma: <3.11 cover
    from typing_extensions import assert_type

import pytest

from academy.agent import action
from academy.agent import Agent
from academy.handle import Handle
from academy.handle import ProxyHandle
from academy.identifier import AgentId
from academy.mypy_plugin import is_handle_type


@pytest.mark.parametrize(
    ('type_name', 'expected'),
    (
        ('academy.handle', False),
        ('academy.handle.Handle', True),
        ('academy.handle.ProxyHandle', True),
        ('academy.handle.Handle[Foo]', True),
        ('academy.handle.Handle[Foo, Bar]', False),
        ('academy.handle.0Handle', False),
    ),
)
def test_is_handle_type(type_name: str, expected: bool) -> None:
    assert is_handle_type(type_name) == expected


class Example(Agent):
    def __init__(self) -> None:
        self.value = 42

    @action
    async def method(self) -> int:
        return 42

    @action
    async def method_with_arg(self, value: int) -> int:
        return value


@pytest.mark.asyncio
async def test_handle_attribute_access_resolved() -> None:
    handle = ProxyHandle(Example())

    # Attributes/method defined on Handle should stay the same
    assert_type(handle.agent_id, AgentId[Example])
    assert_type(await handle.ping(), float)

    # Methods defined on the Agent should have their return type
    # wrapped in a future
    coro = handle.method()
    assert_type(coro, Coroutine[None, None, int])  # type: ignore[unused-coroutine]
    result = await coro
    assert_type(result, int)


@pytest.mark.asyncio
async def test_handle_attribute_access_error() -> None:
    handle = ProxyHandle(Example())

    # Attribute foo does not exist on Example so should raise an error
    with pytest.raises(AttributeError):
        _ = handle.foo  # type: ignore[attr-defined]

    # Method foo does not exist on Example so should raise an error
    with pytest.raises(AttributeError):
        handle.foo()  # type: ignore[attr-defined]

    # Attribute value does exist on Example but is not accessible via a handle
    with pytest.raises(AttributeError):
        _ = handle.value  # type: ignore[attr-defined]

    # Mypy should catch the bad argument type even if it is not an error
    # at runtime
    await handle.method_with_arg('not-an-int')  # type: ignore[arg-type]


def test_handle_union_types() -> None:
    handle1: Example | ProxyHandle[Example] = Example()
    handle2: ProxyHandle[Example] | Example = Example()

    with pytest.raises(AttributeError):
        handle1.ping()  # type: ignore[union-attr]
    with pytest.raises(AttributeError):
        handle2.ping()  # type: ignore[union-attr]

    handle3: Example | ProxyHandle[Example] = ProxyHandle(Example())
    handle4: ProxyHandle[Example] | Example = ProxyHandle(Example())

    # Case 1: Attribute exists on Example but not ProxyHandle[Example]
    with pytest.raises(AttributeError):
        _ = handle3.value  # type: ignore[union-attr]
    with pytest.raises(AttributeError):
        _ = handle4.value  # type: ignore[union-attr]

    # Case 2: Attribute exists on neither Example nor ProxyHandle[Example]
    with pytest.raises(AttributeError):
        _ = handle3.foo  # type: ignore[union-attr]
    with pytest.raises(AttributeError):
        _ = handle4.foo  # type: ignore[union-attr]


@pytest.mark.asyncio
async def test_handle_generic_protocol() -> None:
    # This is testing the casting a Handle implementation (i.e., ProxyHandle)
    # to the protocol Handle type still preserves the method resolution.
    async def _call(handle: Handle[Example]) -> int:
        ping = await handle.ping()
        assert_type(ping, float)

        result = await handle.method()
        assert_type(result, int)
        return result

    handle = ProxyHandle(Example())
    result = await _call(handle)
    assert_type(result, int)
