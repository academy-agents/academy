"""Persistent agent credentials and on-disk store.

Used by the Globus persistent-agent boot path to authenticate as the
agent via ``client_credentials`` without further user interaction.
"""

from __future__ import annotations

import logging
import os
import pathlib
import uuid
from datetime import datetime
from typing import Any
from typing import Generic

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import field_serializer
from pydantic import SecretStr
from pydantic import SerializationInfo

from academy.exchange.cloud.login import get_academy_home
from academy.identifier import AgentId
from academy.identifier import AgentT

logger = logging.getLogger(__name__)

_AGENTS_SUBDIR = 'agents'
_AGENTS_DIR_MODE = 0o700


class PersistentAgentCredentials(BaseModel, Generic[AgentT]):
    """Durable credentials for a Globus persistent agent."""

    agent_id: AgentId[AgentT]
    """Unique identifier for the agent."""

    client_id: uuid.UUID
    """Client ID of the agent's Globus Auth resource server."""

    client_secret: SecretStr
    """Secret used to authenticate the agent's Globus Auth client."""

    scope_string: str
    """Per-agent scope requested via ``client_credentials``."""

    agent_type: tuple[str, ...]
    """MRO of the agent type, matching ``Agent._agent_mro()``."""

    fleet_group_ids: tuple[uuid.UUID, ...]
    """Fleet group memberships at provision time (length 1 in v1)."""

    created_at: datetime
    """Provisioning timestamp."""

    spawn_capable: bool = False
    """Whether this agent may spawn child agents.

    Default ``False``. ``True`` grants project-admin authority and is
    not safe for production until isolation is implemented. 
    """

    model_config = ConfigDict(frozen=True)

    @field_serializer('client_secret', when_used='json')
    def _serialize_client_secret(
        self,
        secret: SecretStr,
        info: SerializationInfo,
    ) -> str:
        if info.context is not None and info.context.get('unmask'):
            return secret.get_secret_value()
        return str(secret)


class AgentCredentialFileStore:
    """JSON-per-agent credential store with 0600 perms and atomic writes.

    Args:
        directory: Storage directory. Defaults to ``$ACADEMY_HOME/agents``.
    """

    def __init__(
        self,
        directory: pathlib.Path | None = None,
    ) -> None:
        if directory is None:
            directory = get_academy_home() / _AGENTS_SUBDIR
        self._dir = pathlib.Path(directory)
        self._dir.mkdir(parents=True, exist_ok=True)
        self._dir.chmod(_AGENTS_DIR_MODE)
        # Clean up any orphaned tmp files from a previous crashed save.
        # A stale ``<uid>.json.tmp`` would otherwise hold the agent's
        # secret on disk.
        for stale in self._dir.glob('*.json.tmp'):
            stale.unlink()

    def _path(self, agent_id: AgentId[Any]) -> pathlib.Path:
        return self._dir / f'{agent_id.uid}.json'

    def save(self, creds: PersistentAgentCredentials[Any]) -> None:
        """Atomically write credentials to ``<agent_id.uid>.json``.

        Uses the ``open(0600) + fsync(fd) + replace + fsync(dir)`` pattern
        so a crash at any point leaves the prior file intact, never a
        partial write.
        """
        if creds.spawn_capable:
            logger.warning(
                'Persisting spawn-capable credentials for %s; not safe '
                'until sub-project grouping lands.',
                creds.agent_id,
                extra={'academy.agent_id': creds.agent_id},
            )
        payload = creds.model_dump_json(context={'unmask': True})
        path = self._path(creds.agent_id)
        tmp = path.with_suffix('.json.tmp')
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        try:
            os.write(fd, payload.encode('utf-8'))
            os.fsync(fd)
        finally:
            os.close(fd)
        os.replace(tmp, path)
        dir_fd = os.open(self._dir, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)

    def load(
        self,
        agent_id: AgentId[Any],
    ) -> PersistentAgentCredentials[Any]:
        """Load credentials for an agent.

        Raises:
            FileNotFoundError: if no credentials are stored for the agent.
        """
        return PersistentAgentCredentials.model_validate_json(
            self._path(agent_id).read_text(encoding='utf-8'),
        )

    def list_agents(self) -> list[AgentId[Any]]:
        """List identifiers for all agents with credentials on disk.

        Returns ``AgentId`` instances with ``name=None``. Call
        ``load(agent_id)`` if the display name is needed.
        """
        return [
            AgentId(uid=uuid.UUID(p.stem))
            for p in sorted(self._dir.glob('*.json'))
        ]

    def delete(self, agent_id: AgentId[Any]) -> None:
        """Remove credentials for an agent.

        Raises:
            FileNotFoundError: if no credentials are stored for the agent.
        """
        self._path(agent_id).unlink()
