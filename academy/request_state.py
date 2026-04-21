from __future__ import annotations

import dataclasses

from academy.identifier import EntityId


@dataclasses.dataclass
class RequestInfo:
    """Metadata for an in-flight request."""

    src: EntityId
    dest: EntityId
