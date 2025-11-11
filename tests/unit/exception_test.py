from __future__ import annotations

import pickle

import pytest

from academy.exception import ActionCancelledError
from academy.exception import AgentNotInitializedError
from academy.exception import AgentTerminatedError
from academy.exception import BadEntityIdError
from academy.exception import ExchangeClientNotFoundError
from academy.exception import ExchangeError
from academy.exception import ForbiddenError
from academy.exception import MailboxTerminatedError
from academy.exception import MessageTooLargeError
from academy.exception import PingCancelledError
from academy.exception import UnauthorizedError
from academy.exception import UserTerminatedError
from academy.identifier import AgentId
from academy.identifier import UserId


@pytest.mark.parametrize(
    'exc',
    (
        ActionCancelledError('test'),
        AgentNotInitializedError(),
        ExchangeError(),
        BadEntityIdError(AgentId.new()),
        ForbiddenError(),
        MessageTooLargeError(2, 1),
        MailboxTerminatedError(AgentId.new()),
        AgentTerminatedError(AgentId.new()),
        UserTerminatedError(UserId.new()),
        UnauthorizedError(),
        ExchangeClientNotFoundError(AgentId.new()),
        PingCancelledError(),
    ),
)
def test_pickle_exception(exc: Exception) -> None:
    pkl = pickle.dumps(exc)
    reconstructed = pickle.loads(pkl)
    # Exception types cannot be tested for equality using == so use the
    # repr as a proxy for equality.
    assert repr(exc) == repr(reconstructed)
