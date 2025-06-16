from __future__ import annotations

import uuid
from typing import Any

from academy.identifier import AgentId
from academy.identifier import UserId


def tests_identifier_equality() -> None:
    aid: AgentId[Any] = AgentId.new()
    cid = UserId.new()

    assert isinstance(aid, AgentId)
    assert isinstance(cid, UserId)

    assert aid != cid
    assert hash(aid) != hash(cid)

    uid = uuid.uuid4()
    aid1: AgentId[Any] = AgentId(uid=uid)
    aid2: AgentId[Any] = AgentId(uid=uid)
    cid1 = UserId(uid=uid)

    assert aid1 == aid2
    assert str(aid1) == str(aid2)
    assert aid1 != cid1


def tests_identifier_equality_ignore_name() -> None:
    uid = uuid.uuid4()
    aid1: AgentId[Any] = AgentId(uid=uid, name='aid1')
    aid2: AgentId[Any] = AgentId(uid=uid, name='aid2')

    assert aid1 == aid2
    assert str(aid1) != str(aid2)
