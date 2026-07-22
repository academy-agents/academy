from __future__ import annotations

import pathlib
import pickle
import uuid
from datetime import datetime
from typing import Any
from unittest import mock

import pytest
from pydantic import SecretStr
from pydantic import ValidationError

from academy.exchange.cloud.credentials import AgentCredentialFileStore
from academy.exchange.cloud.credentials import PersistentAgentCredentials
from academy.identifier import AgentId


def _make_creds(
    *,
    name: str = 'test',
    client_secret: str = 'secret',
    spawn_capable: bool = False,
) -> PersistentAgentCredentials[Any]:
    return PersistentAgentCredentials(
        agent_id=AgentId.new(name=name),
        client_id=uuid.uuid4(),
        client_secret=SecretStr(client_secret),
        scope_string='launch',
        agent_type=('module.Cls', 'module.Base'),
        fleet_group_ids=(uuid.uuid4(),),
        created_at=datetime.now(),
        spawn_capable=spawn_capable,
    )


def test_credentials_pickle_roundtrip() -> None:
    creds = _make_creds()
    assert pickle.loads(pickle.dumps(creds)) == creds


def test_credentials_is_frozen() -> None:
    creds = _make_creds()
    with pytest.raises(ValidationError):
        creds.client_secret = SecretStr('mutated')  # type: ignore[misc]


def test_credentials_secret_does_not_leak() -> None:
    creds = _make_creds(client_secret='qwerty')
    assert 'qwerty' not in repr(creds)
    assert 'qwerty' not in creds.model_dump_json()


def test_credentials_spawn_capable_defaults_false() -> None:
    # Constructed directly (not via _make_creds) so the model's default
    # is exercised, not the helper's.
    creds: PersistentAgentCredentials[Any] = PersistentAgentCredentials(
        agent_id=AgentId.new(),
        client_id=uuid.uuid4(),
        client_secret=SecretStr('x'),
        scope_string='launch',
        agent_type=('A',),
        fleet_group_ids=(uuid.uuid4(),),
        created_at=datetime.now(),
    )
    assert creds.spawn_capable is False


def test_store_save_load(tmp_path: pathlib.Path) -> None:
    store = AgentCredentialFileStore(tmp_path)
    creds = _make_creds()
    store.save(creds)
    assert store.load(creds.agent_id) == creds


def test_store_save_load_with_spawn_capable(tmp_path: pathlib.Path) -> None:
    store = AgentCredentialFileStore(tmp_path)
    creds = _make_creds(spawn_capable=True)
    store.save(creds)
    assert store.load(creds.agent_id).spawn_capable is True


def test_store_uses_default_dir_under_academy_home(
    tmp_path: pathlib.Path,
) -> None:
    with mock.patch(
        'academy.exchange.cloud.credentials.get_academy_home',
        return_value=tmp_path,
    ):
        AgentCredentialFileStore()
    assert (tmp_path / 'agents').is_dir()


def test_store_save_uses_0600(tmp_path: pathlib.Path) -> None:
    store = AgentCredentialFileStore(tmp_path)
    creds = _make_creds()
    store.save(creds)
    path = tmp_path / f'{creds.agent_id.uid}.json'
    assert path.stat().st_mode & 0o777 == 0o600  # noqa: PLR2004


def test_store_dir_perms_are_0o700(tmp_path: pathlib.Path) -> None:
    target = tmp_path / 'agents'
    AgentCredentialFileStore(target)
    assert target.stat().st_mode & 0o777 == 0o700  # noqa: PLR2004


def test_store_cleans_orphaned_tmp_files(tmp_path: pathlib.Path) -> None:
    # A stale .json.tmp from a crashed save would otherwise hold the
    # agent's secret on disk indefinitely.
    orphan = tmp_path / f'{uuid.uuid4()}.json.tmp'
    orphan.write_text('{"stale": true}')
    AgentCredentialFileStore(tmp_path)
    assert not orphan.exists()


def test_store_list_agents(tmp_path: pathlib.Path) -> None:
    store = AgentCredentialFileStore(tmp_path)
    a = _make_creds(name='a')
    b = _make_creds(name='b')
    store.save(a)
    store.save(b)
    listed = store.list_agents()
    assert {aid.uid for aid in listed} == {a.agent_id.uid, b.agent_id.uid}


def test_store_delete(tmp_path: pathlib.Path) -> None:
    store = AgentCredentialFileStore(tmp_path)
    creds = _make_creds()
    store.save(creds)
    store.delete(creds.agent_id)
    with pytest.raises(FileNotFoundError):
        store.load(creds.agent_id)


def test_store_delete_missing(tmp_path: pathlib.Path) -> None:
    store = AgentCredentialFileStore(tmp_path)
    with pytest.raises(FileNotFoundError):
        store.delete(AgentId.new())
