from __future__ import annotations

import threading
import time

import pytest

from academy.thread import ThreadWithErrorHandling
from testing.constant import TEST_THREAD_JOIN_TIMEOUT


def test_thread_success():
    called = threading.Event()

    def target():
        called.set()

    thread = ThreadWithErrorHandling(target)
    thread.join(TEST_THREAD_JOIN_TIMEOUT)

    assert called.is_set()


def test_thread_failure():
    def target():
        raise RuntimeError('Oops!')

    thread = ThreadWithErrorHandling(target)

    with pytest.raises(RuntimeError, match='Oops!'):
        thread.join(TEST_THREAD_JOIN_TIMEOUT)


def test_thread_join_timeout():
    def target():
        time.sleep(0.15)

    thread = ThreadWithErrorHandling(target)

    with pytest.raises(TimeoutError):
        thread.join(timeout=0.001)

    thread.join(TEST_THREAD_JOIN_TIMEOUT)
