from __future__ import annotations

import asyncio
import random
import socket
import string
import sys
from collections.abc import AsyncGenerator
from unittest import mock

import pytest
import pytest_asyncio

from academy.socket import _BAD_FILE_DESCRIPTOR_ERRNO
from academy.socket import _make_header
from academy.socket import address_by_hostname
from academy.socket import address_by_interface
from academy.socket import MESSAGE_CHUNK_SIZE
from academy.socket import open_port
from academy.socket import SimpleSocket
from academy.socket import SimpleSocketServer
from academy.socket import SocketClosedError
from academy.socket import SocketOpenError
from academy.socket import SocketPool
from academy.socket import wait_connection


@pytest_asyncio.fixture(loop_scope='module')
async def echo_server() -> AsyncGenerator[tuple[str, int]]:
    async def _handler(
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        data = await reader.read(100)
        writer.write(data)
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    host, port = 'localhost', open_port()
    server = await asyncio.start_server(_handler, host, port)

    async with server:
        await server.start_serving()
        yield host, port


@pytest.mark.parametrize('shutdown', (True, False))
@pytest.mark.asyncio(loop_scope='module')
async def test_create_simple_socket(
    echo_server: tuple[str, int],
    shutdown: bool,
) -> None:
    host, port = echo_server
    async with await SimpleSocket.connect(host, port) as socket:
        assert isinstance(repr(socket), str)
        assert isinstance(str(socket), str)

        await socket.close(shutdown=shutdown)
        with pytest.raises(SocketClosedError):
            await socket.recv()


@pytest.mark.asyncio
async def test_create_simple_socket_error() -> None:
    with mock.patch('asyncio.open_connection', side_effect=OSError()):
        with pytest.raises(SocketOpenError):
            await SimpleSocket.connect('localhost', 0)


@pytest.mark.asyncio(loop_scope='module')
async def test_simple_socket_send_recv(echo_server: tuple[str, int]) -> None:
    host, port = echo_server
    async with await SimpleSocket.connect(host, port) as socket:
        # Should be a no-op
        await socket.send(b'')

        message = 'hello, world!'
        await socket.send_string(message)
        assert await socket.recv_string() == message

        error = OSError(_BAD_FILE_DESCRIPTOR_ERRNO, 'Bad file descriptor.')
        with mock.patch.object(socket.writer, 'write', side_effect=error):
            with pytest.raises(  # pragma: <3.14 cover
                OSError,
                match=r'Bad file descriptor\.',
            ):
                await socket.send_string('hello, again!')


@pytest.mark.asyncio(loop_scope='module')
async def test_simple_socket_send_multipart(
    echo_server: tuple[str, int],
) -> None:
    host, port = echo_server
    size = int(2.5 * MESSAGE_CHUNK_SIZE)
    message = ''.join(
        [random.choice(string.ascii_uppercase) for _ in range(size)],
    )
    async with await SimpleSocket.connect(host, port) as socket:
        with mock.patch.object(socket.writer, 'write') as mock_write:
            await socket.send_string(message)
            assert mock_write.call_count == 4  # noqa: PLR2004


@pytest.mark.asyncio(loop_scope='module')
async def test_simple_socket_recv_multipart(
    echo_server: tuple[str, int],
) -> None:
    host, port = echo_server
    async with await SimpleSocket.connect(host, port) as socket:
        messages = [b'hello, world!', b'part2']
        with mock.patch.object(socket.reader, 'readexactly') as mock_read:
            # Mock received parts to include two messages split across three
            # parts followed by a socket close event.
            mock_read.side_effect = [
                _make_header(messages[0]),
                b'hello, ',
                b'world!',
                _make_header(messages[1]),
                messages[1],
                SocketClosedError(),
            ]

            assert await socket.recv_string() == 'hello, world!'
            assert await socket.recv_string() == 'part2'
            with pytest.raises(SocketClosedError):  # pragma: <3.14 cover
                assert await socket.recv_string()


@pytest.mark.asyncio(loop_scope='module')
async def test_socket_pool_send_and_reuse_socket(
    echo_server: tuple[str, int],
) -> None:
    host, port = echo_server
    address = f'{host}:{port}'

    pool = SocketPool()
    message = b'hello'

    await pool.send(address, message)
    await pool.send(address, message)

    assert address in pool._sockets
    assert len(pool._sockets) == 1

    await pool.close()


@pytest.mark.asyncio(loop_scope='module')
async def test_socket_pool_close_removes_socket(echo_server: tuple[str, int]):
    host, port = echo_server
    address = f'{host}:{port}'

    pool = SocketPool()
    await pool.send(address, b'ping')

    assert address in pool._sockets

    await pool.close_socket(address)
    assert address not in pool._sockets


@pytest.mark.asyncio(loop_scope='module')
async def test_pool_close_closes_all(echo_server: tuple[str, int]):
    host, port = echo_server
    address = f'{host}:{port}'

    pool = SocketPool()
    await pool.send(address, b'ping')
    await pool.close()

    assert len(pool._sockets) == 0


@pytest_asyncio.fixture(loop_scope='module')
async def simple_socket_server() -> AsyncGenerator[SimpleSocketServer]:
    async def _identity(x: bytes) -> bytes:
        return x

    server = SimpleSocketServer(handler=_identity, host='localhost', port=None)
    async with server.serve():
        yield server


@pytest.mark.asyncio(loop_scope='module')
async def test_simple_socket_server_connect(
    simple_socket_server: SimpleSocketServer,
) -> None:
    for _ in range(3):
        async with await SimpleSocket.connect(
            simple_socket_server.host,
            simple_socket_server.port,
        ):
            pass


@pytest.mark.asyncio(loop_scope='module')
async def test_simple_socket_server_ping_pong(
    simple_socket_server: SimpleSocketServer,
) -> None:
    message = 'hello, world!'
    async with await SimpleSocket.connect(
        simple_socket_server.host,
        simple_socket_server.port,
    ) as socket:
        for _ in range(3):
            await socket.send_string(message)
            assert await socket.recv_string() == message


@pytest.mark.asyncio(loop_scope='module')
async def test_simple_socket_server_packed(
    simple_socket_server: SimpleSocketServer,
) -> None:
    # Pack many messages into one buffer to be send
    messages = [b'first message', b'seconds message', b'third message']
    buffer = b''.join(_make_header(m) + m for m in messages)

    async with await SimpleSocket.connect(
        simple_socket_server.host,
        simple_socket_server.port,
    ) as socket:
        socket.writer.write(buffer)
        await socket.writer.drain()
        for expected in messages:
            assert await socket.recv() == expected


@pytest.mark.asyncio(loop_scope='module')
async def test_simple_socket_server_multipart(
    simple_socket_server: SimpleSocketServer,
) -> None:
    # Generate >1024 bytes of data since _recv_from_socket reads in 1kB
    # chunks. This test forces recv() to buffer incomplete chunks.
    first_parts = [random.randbytes(500) for _ in range(3)]
    second_part = b'second message!'
    # socket.recv_string() will not return the delimiter so add after
    # computing the expected string
    first_expected = b''.join(first_parts)
    parts = [
        _make_header(first_expected),
        first_parts[0],
        first_parts[1],
        first_parts[2],
        _make_header(second_part),
        second_part,
    ]

    async with await SimpleSocket.connect(
        simple_socket_server.host,
        simple_socket_server.port,
    ) as socket:
        for part in parts:
            socket.writer.write(part)
        await socket.writer.drain()
        assert await socket.recv() == first_expected
        assert await socket.recv() == second_part


@pytest.mark.asyncio(loop_scope='module')
async def test_simple_socket_server_client_disconnect_early(
    simple_socket_server: SimpleSocketServer,
) -> None:
    async with await SimpleSocket.connect(
        simple_socket_server.host,
        simple_socket_server.port,
    ):
        # Client disconnects without sending anything
        pass


def test_get_address_by_hostname() -> None:
    assert isinstance(address_by_hostname(), str)


@pytest.mark.skipif(
    sys.platform == 'darwin',
    reason='Test does not run on darwin',
)
def test_get_address_by_interface() -> None:  # pragma: darwin no cover
    for _, ifname in socket.if_nameindex():
        try:
            assert isinstance(address_by_interface(ifname), str)
        except Exception:  # pragma: no cover
            continue
        else:
            break
    else:  # pragma: no cover
        raise RuntimeError('Failed to find a valid address by interface.')


def test_wait_connection() -> None:
    with mock.patch('socket.create_connection'):
        wait_connection('localhost', port=0)


def test_wait_connection_timeout() -> None:
    with mock.patch('socket.create_connection', side_effect=OSError()):
        with pytest.raises(TimeoutError):
            wait_connection('localhost', port=0, sleep=0, timeout=0)

        with pytest.raises(TimeoutError):
            wait_connection('localhost', port=0, sleep=0.01, timeout=0.05)
