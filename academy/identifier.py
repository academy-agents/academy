from __future__ import annotations

import sys
import uuid
from typing import Any
from typing import Generic
from typing import Literal
from typing import Optional
from typing import TypeVar
from typing import Union

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field

# Normally this would be bound=Behavior, but Pydantic's mypy plugin crashes
# here. See https://github.com/pydantic/pydantic/issues/11454
BehaviorT = TypeVar('BehaviorT')


class AgentId(BaseModel, Generic[BehaviorT]):
    """Unique identifier for an agent entity in a multi-agent system."""

    uid: uuid.UUID = Field()
    name: Optional[str] = Field(None)  # noqa: UP007
    role: Literal['agent'] = Field('agent', repr=False)

    model_config = ConfigDict(
        extra='forbid',
        frozen=True,
        validate_default=True,
    )

    def __eq__(self, other: object, /) -> bool:
        return isinstance(other, AgentId) and self.uid == other.uid

    def __hash__(self) -> int:
        return hash(self.role) + hash(self.uid)

    def __str__(self) -> str:
        name = self.name if self.name is not None else str(self.uid)[:8]
        return f'AgentId<{name}>'

    @classmethod
    def new(cls, name: str | None = None) -> Self:
        """Create a new identifier.

        Args:
            name: Optional human-readable name for the entity.
        """
        return cls(uid=uuid.uuid4(), name=name)


class UserId(BaseModel):
    """Unique identifier for a user entity in a multi-agent system."""

    uid: uuid.UUID = Field()
    name: Optional[str] = Field(None)  # noqa: UP007
    role: Literal['user'] = Field('user', repr=False)

    model_config = ConfigDict(
        extra='forbid',
        frozen=True,
        validate_default=True,
    )

    def __eq__(self, other: object, /) -> bool:
        return isinstance(other, UserId) and self.uid == other.uid

    def __hash__(self) -> int:
        return hash(self.role) + hash(self.uid)

    def __str__(self) -> str:
        name = self.name if self.name is not None else str(self.uid)[:8]
        return f'UserId<{name}>'

    @classmethod
    def new(cls, name: str | None = None) -> Self:
        """Create a new identifier.

        Args:
            name: Optional human-readable name for the entity.
        """
        return cls(uid=uuid.uuid4(), name=name)


EntityId = Union[AgentId[Any], UserId]
"""EntityId union type for type annotations."""
