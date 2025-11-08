"""Academy mypy plugin.

This plugin enables mypy to perform static type inference on
[`Handle`][academy.handle.Handle] types.

```python
from academy.agent import Agent, action
from academy.handle import Handle

class Example(Agent):
    @action
    async def get_value(self) -> int: ...

handle: Handle[Example]

reveal_type(await handle.get_value())
# note: Revealed type is "int"
```
Without the plugin, mypy will default to [`Any`][typing.Any].

Note:
    The plugin makes a best effort to warn users about incorrect use of
    agent handles. This includes raising errors when accessing attributes
    of a agent, rather than methods, via a handle and when incorrect
    parameter types are passed to an action. However, the plugin cannot
    distinguish which callable attributes of a handle are annotated as
    actions, but this will still produce an error at runtime.

Enable the plugin by adding `academy.mypy_plugin` to the list of plugins
in your
[mypy config file](https://mypy.readthedocs.io/en/latest/config_file.html){target=_blank}.

* `pyproject.toml`
  ```toml
  [tools.mypy]
  plugins = ["academy.mypy_plugin"]
  ```
* `mypy.ini` and `setup.cfg`
  ```ini
  [mypy]
  plugins = academy.mypy_plugin
  ```
"""

from __future__ import annotations

import functools
import re
from collections.abc import Callable
from typing import ParamSpec
from typing import TypeVar

from mypy.checker import TypeChecker
from mypy.errorcodes import ATTR_DEFINED
from mypy.errorcodes import UNION_ATTR
from mypy.messages import format_type
from mypy.options import Options
from mypy.plugin import AttributeContext
from mypy.plugin import MethodContext
from mypy.plugin import Plugin
from mypy.types import AnyType
from mypy.types import CallableType
from mypy.types import get_proper_type
from mypy.types import Instance
from mypy.types import NoneType
from mypy.types import Type
from mypy.types import TypeOfAny
from mypy.types import UnionType

P = ParamSpec('P')
T = TypeVar('T')


HANDLE_TYPE_PATTERN = (
    r'^academy\.handle\.([a-zA-Z_][a-zA-Z0-9_]*)?Handle'
    r'(?:\[\s*[a-zA-Z_][a-zA-Z0-9_]*\s*\])?$'
)


def is_handle_type(type_name: str) -> bool:  # noqa: D103
    # The HANDLE_TYPE_PATTERN assumes all handle types are defined in the
    # academy.handle module. If other handle types are ever introduced this
    # utility function will need to be changed to enable mypy inference.
    return bool(re.match(HANDLE_TYPE_PATTERN, type_name))


class AcademyMypyPlugin(Plugin):  # noqa: D101
    def __init__(self, options: Options) -> None:
        super().__init__(options)

    def get_attribute_hook(  # noqa: D102
        self,
        fullname: str,
    ) -> Callable[[AttributeContext], Type] | None:
        return self._get_handle_hook(fullname)

    def get_method_hook(  # noqa: D102
        self,
        fullname: str,
    ) -> Callable[[MethodContext], Type] | None:
        return self._get_handle_hook(fullname)

    def _get_handle_hook(
        self,
        fullname: str,
    ) -> Callable[[AttributeContext | MethodContext], Type] | None:
        parts = fullname.rsplit('.', 1)
        if len(parts) != 2:  # noqa: PLR2004
            return None
        type_name, attr_name = parts
        if is_handle_type(type_name):
            return functools.partial(
                handle_attr_access,
                attr_name=attr_name,
            )
        return None


def _assertion_fallback(function: Callable[P, Type]) -> Callable[P, Type]:
    # Decorator which catches AssertionErrors and returns AnyType
    # to indicate that the plugin does not know how to handle that case
    # and will default back to Any.
    # https://github.com/dry-python/returns/blob/dda187d78fe405d7d1234ffaffc99d8264f854dc/returns/contrib/mypy/_typeops/fallback.py
    @functools.wraps(function)
    def decorator(*args: P.args, **kwargs: P.kwargs) -> Type:
        try:
            return function(*args, **kwargs)
        except AssertionError:
            return AnyType(TypeOfAny.implementation_artifact)

    return decorator


def _handle_attr_access(  # noqa: C901, PLR0911, PLR0912
    instance: Type,
    attr_name: str,
    ctx: AttributeContext | MethodContext,
) -> Type:
    handle_type = get_proper_type(instance)

    if isinstance(ctx, MethodContext):
        fallback_type = ctx.default_return_type
    elif isinstance(ctx, AttributeContext):
        fallback_type = ctx.default_attr_type

    if not isinstance(handle_type, Instance):
        return fallback_type

    handle_typename = handle_type.type.fullname
    if not is_handle_type(handle_typename):
        return fallback_type

    # Attribute/method defined directly on Handle itself
    if handle_type.type.has_readable_member(attr_name):
        return fallback_type

    # After the above checks, we know instance is a Handle type and Handle
    # types are generic with one generic type.
    assert len(handle_type.args) == 1
    bound_type = get_proper_type(handle_type.args[0])
    if not isinstance(bound_type, Instance):
        # This could, for example, be a TypeVarType if we have an unbound
        # Handle[T].
        return fallback_type

    bound_attr_sym = bound_type.type.get(attr_name)
    if (
        not bound_attr_sym
        or not bound_attr_sym.node
        or not hasattr(bound_attr_sym.node, 'type')
    ):
        type_ = format_type(handle_type, ctx.api.options)
        code = ATTR_DEFINED
        if isinstance(ctx.type, UnionType):
            union = format_type(ctx.type, ctx.api.options)
            type_ = f'Item {type_} of {union}'
            code = UNION_ATTR
        ctx.api.fail(
            f'{type_} has no attribute "{attr_name}"',
            ctx.context,
            code=code,
        )
        return fallback_type

    bound_attr_type = get_proper_type(bound_attr_sym.type)
    if not isinstance(bound_attr_type, CallableType):
        if bound_attr_type is not None:
            handle_type_ = format_type(handle_type, ctx.api.options)
            bound_type_ = format_type(bound_type, ctx.api.options)
            code = ATTR_DEFINED
            if isinstance(ctx.type, UnionType):
                union = format_type(ctx.type, ctx.api.options)
                handle_type_ = f'Item {handle_type_} of {union}'
                code = UNION_ATTR
            ctx.api.fail(
                f'{handle_type_} has no method "{attr_name}"; '
                f'only action-decorated methods of {bound_type_} '
                'can accessed via a handle',
                ctx.context,
                code=code,
            )
        return fallback_type

    ret_type: Type
    ret_ret_type = get_proper_type(bound_attr_type.ret_type)
    if (
        isinstance(ret_ret_type, Instance)
        and ret_ret_type.type.fullname == 'typing.Coroutine'
        and len(ret_ret_type.args) == 3  # noqa: PLR2004 (Coroutine[A, B, C])
    ):
        inner_type = ret_ret_type.args[2]
        assert isinstance(ctx.api, TypeChecker)
        coroutine_type = ctx.api.named_type('typing.Coroutine')
        ret_type = coroutine_type.copy_modified(
            # Coroutine[None, None, T]
            args=[NoneType(), NoneType(), inner_type],
        )
    else:
        return fallback_type

    if isinstance(ctx, AttributeContext):
        # Need to drop the "self" argument so that mypy does not think
        # it was forgotten.
        return bound_attr_type.copy_modified(
            arg_types=bound_attr_type.arg_types[1:],
            arg_kinds=bound_attr_type.arg_kinds[1:],
            arg_names=bound_attr_type.arg_names[1:],
            ret_type=ret_type,
        )
    elif isinstance(ctx, MethodContext):
        return ret_type


@_assertion_fallback
def handle_attr_access(  # noqa: D103
    ctx: AttributeContext | MethodContext,
    attr_name: str,
) -> Type:
    if isinstance(ctx.type, UnionType):
        resolved = tuple(
            _handle_attr_access(instance, attr_name, ctx)
            for instance in ctx.type.items
        )
        return UnionType(resolved)
    else:
        return _handle_attr_access(ctx.type, attr_name, ctx)


def plugin(version: str) -> type[AcademyMypyPlugin]:  # noqa: D103
    return AcademyMypyPlugin
