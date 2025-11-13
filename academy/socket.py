from __future__ import annotations

import asyncio
import contextlib
import fcntl
import logging
import platform
import socket
import struct
import sys
import time
from collections.abc import AsyncGenerator
from collections.abc import Awaitable
from collections.abc import Callable
from contextlib import asynccontextmanager
from types import TracebackType

from academy.task import spawn_guarded_background_task

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

logger = logging.getLogger(__name__)

_BAD_FILE_DESCRIPTOR_ERRNO = 9
MESSAGE_CHUNK_SIZE = 10240
MESSAGE_HEADER_FORMAT = '!I'  # Network byte order, unsigned 4-byte int
MESSAGE_HEADER_SIZE = struct.calcsize(MESSAGE_HEADER_FORMAT)


class SocketClosedError(Exception):
    """Socket is already closed."""

    pass


class SocketOpenError(Exception):
    """Failed to open socket."""

    pass


class SimpleSocket:
    """Simple socket wrapper.

    Configures a client connection using a non-blocking TCP socket over IPv4.
    The send and recv methods handle byte encoding, message delimiters, and
    partial message buffering.

    Note:
        This class can be used as an async context manager.

    Args:
        reader: Socket reader interface.
        writer: Socket writer interface.
        timeout: Optional timeout for socket operations.
    """

    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        *,
        timeout: float | None = None,
    ) -> None:
        self.reader = reader
        self.writer = writer
        self.timeout = timeout
        self.closed = False

    @classmethod
    async def connect(
        cls,
        host: str,
        port: int,
        *,
        timeout: float | None = None,
    ) -> Self:
        """Establish a new TCP connection.

        Args:
            host: Host address to connect to.
            port: Port to connect to.
            timeout: Connection establish timeout.

        Raises:
            SocketOpenError: If creating the socket fails. The `__cause__` of
                the exception will be set to the underlying `OSError`.
        """
        try:
            coro = asyncio.open_connection(host, port)
            reader, writer = await asyncio.wait_for(coro, timeout)
        except (OSError, asyncio.TimeoutError) as e:
            raise SocketOpenError() from e

        return cls(reader, writer, timeout=timeout)

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.close()

    def __repr__(self) -> str:
        return f'{type(self).__name__}({self.reader!r}, {self.writer!r})'

    def __str__(self) -> str:
        return f'{type(self).__name__}<{self.reader}, {self.writer}>'

    async def close(self, shutdown: bool = True) -> None:
        """Close the socket.

        Args:
            shutdown: Write EOF to the socket.
        """
        if self.closed:
            return
        self.closed = True
        if shutdown and not self.writer.is_closing():
            self.writer.write_eof()
        self.writer.close()
        await self.writer.wait_closed()

    async def send(self, message: bytes) -> None:
        """Send bytes to the socket.

        Note:
            This is a noop if the message is empty.

        Args:
            message: Message to send.

        Raises:
            SocketClosedError: If the socket was closed.
            OSError: If an error occurred.
        """
        if self.closed or self.writer.is_closing():
            raise SocketClosedError()

        if not message:
            return

        header = _make_header(message)
        self.writer.write(header)
        await self.writer.drain()

        sent_size = 0
        message_size = len(message)
        while sent_size < message_size:
            nbytes = min(message_size - sent_size, MESSAGE_CHUNK_SIZE)
            chunk = message[sent_size : sent_size + nbytes]
            self.writer.write(chunk)
            await self.writer.drain()
            sent_size += len(chunk)

    async def send_string(self, message: str) -> None:
        """Send a string to the socket.

        Strings are encoded with UTF-8.

        Args:
            message: Message to send.

        Raises:
            SocketClosedError: if the socket was closed.
            OSError: if an error occurred.
        """
        await self.send(message.encode('utf-8'))

    async def recv(self) -> bytes:
        """Receive the next message from the socket.

        Returns:
            Bytes containing the message.

        Raises:
            SocketClosedError: if the socket was closed.
            OSError: if an error occurred.
        """
        if self.closed:
            raise SocketClosedError()

        header = await self.reader.readexactly(MESSAGE_HEADER_SIZE)
        message_size = _get_size_from_header(header)

        buffer = bytearray(message_size)
        received = 0
        while received < message_size:
            nbytes = min(message_size - received, MESSAGE_CHUNK_SIZE)
            chunk = await self.reader.readexactly(nbytes)
            buffer[received : received + len(chunk)] = chunk
            received += len(chunk)

        return buffer

    async def recv_string(self) -> str:
        """Receive the next message from the socket.

        Returns:
            Message decoded as a UTF-8 string.

        Raises:
            SocketClosedError: if the socket was closed.
            OSError: if an error occurred.
        """
        return (await self.recv()).decode('utf-8')


class SocketPool:
    """Simple socket pool."""

    def __init__(self) -> None:
        self._sockets: dict[str, SimpleSocket] = {}
        self._lock = asyncio.Lock()

    async def close(self) -> None:
        """Close all the sockets in the pool."""
        for address in tuple(self._sockets.keys()):
            await self.close_socket(address)

    async def close_socket(self, address: str) -> None:
        """Close the socket for the given address."""
        async with self._lock:
            conn = self._sockets.pop(address, None)
        if conn is not None:  # pragma: no branch
            await conn.close(shutdown=True)

    async def get_socket(self, address: str) -> SimpleSocket:
        """Get or create a socket for a given address."""
        async with self._lock:
            try:
                return self._sockets[address]
            except KeyError:
                parts = address.split(':')
                host, port = parts[0], int(parts[1])
                conn = await SimpleSocket.connect(host, port)
                self._sockets[address] = conn
                return conn

    async def send(self, address: str, message: bytes) -> None:
        """Send a message to a given address."""
        conn = await self.get_socket(address)
        try:
            await conn.send(message)
        except (SocketClosedError, OSError):
            await self.close_socket(address)
            raise


class SimpleSocketServer:
    """Simple asyncio TCP socket server.

    Args:
        handler: Callback that handles a message and returns the response
            string. The handler is called synchronously within the client
            handler so it should not perform any heavy/blocking operations.
        host: Host to bind to.
        port: Port to bind to. If `None`, a random port is bound to.
    """

    def __init__(
        self,
        handler: Callable[[bytes], Awaitable[bytes | None]],
        *,
        host: str = '0.0.0.0',
        port: int | None = None,
    ) -> None:
        self.host = host
        self.port = port if port is not None else open_port()
        self.handler = handler
        self._client_tasks: set[asyncio.Task[None]] = set()

    async def _read_message(
        self,
        reader: asyncio.StreamReader,
    ) -> bytes | bytearray:
        header = await reader.read(MESSAGE_HEADER_SIZE)
        if len(header) == 0:  # pragma: no cover
            return b''
        message_size = _get_size_from_header(header)

        buffer = bytearray(message_size)
        received = 0
        while received < message_size:
            nbytes = min(message_size - received, MESSAGE_CHUNK_SIZE)
            chunk = await reader.read(nbytes)
            # buffer.extend(chunk)
            buffer[received : received + len(chunk)] = chunk
            received += len(chunk)

        return buffer

    async def _write_message(
        self,
        writer: asyncio.StreamWriter,
        message: bytes | bytearray,
    ) -> None:
        message_size = len(message)
        header = _make_header(message)
        writer.write(header)

        sent_size = 0
        while sent_size < message_size:
            nbytes = min(message_size - sent_size, MESSAGE_CHUNK_SIZE)
            chunk = message[sent_size : sent_size + nbytes]
            writer.write(chunk)
            await writer.drain()
            sent_size += len(chunk)

        await writer.drain()

    async def _handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        try:
            while not reader.at_eof():  # pragma: no branch
                try:
                    message = await self._read_message(reader)
                except asyncio.IncompleteReadError:  # pragma: no cover
                    reader.feed_eof()
                    continue
                else:
                    if len(message) == 0:  # pragma: no cover
                        break
                    response = await self.handler(message)
                    if response is not None:  # pragma: no break
                        await self._write_message(writer, response)
        except Exception:  # pragma: no cover
            logger.exception('Error in client handler task.')
            raise
        finally:
            writer.close()
            await writer.wait_closed()

    async def _register_client_task(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        task = spawn_guarded_background_task(
            self._handle_client(reader, writer),
            name=f'server-{self.host}:{self.port}-handler',
            log_exception=False,
        )
        self._client_tasks.add(task)
        task.add_done_callback(self._client_tasks.discard)

    @asynccontextmanager
    async def serve(self) -> AsyncGenerator[Self]:
        """Serve in a context manager."""
        server = await asyncio.start_server(
            self._register_client_task,
            host=self.host,
            port=self.port,
        )
        logger.debug(
            'TCP server listening at %s:%s',
            self.host,
            self.port,
            extra={'academy.host': self.host, 'academy.port': self.port},
        )

        async with server:
            await server.start_serving()

            yield self

            for task in tuple(self._client_tasks):
                task.cancel('Server has been closed.')
                with contextlib.suppress(asyncio.CancelledError):
                    await task

        if sys.version_info >= (3, 13):  # pragma: >=3.13 cover
            server.close_clients()
        logger.debug(
            'TCP server finished at %s:%s',
            self.host,
            self.port,
            extra={'academy.host': self.host, 'academy.port': self.port},
        )


def _get_size_from_header(header: bytes) -> int:
    return struct.unpack(MESSAGE_HEADER_FORMAT, header)[0]


def _make_header(message: bytes | bytearray) -> bytes:
    return struct.pack(MESSAGE_HEADER_FORMAT, len(message))


def address_by_hostname() -> str:
    """Get the IP address from the hostname of the local host."""
    return socket.gethostbyname(platform.node())


def address_by_interface(ifname: str) -> str:  # pragma: darwin no cover
    """Get the IP address of the given interface.

    Source: https://stackoverflow.com/questions/24196932/how-can-i-get-the-ip-address-of-eth0-in-python#24196955

    Args:
        ifname: Name of the interface whose address is to be returned.
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    return socket.inet_ntoa(
        fcntl.ioctl(
            s.fileno(),
            0x8915,  # SIOCGIFADDR
            struct.pack('256s', bytes(ifname[:15], 'utf-8')),
        )[20:24],
    )


_used_ports: set[int] = set()


def open_port() -> int:
    """Return open port.

    Source: https://stackoverflow.com/questions/2838244
    """
    while True:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(('', 0))
        s.listen(1)
        port = s.getsockname()[1]
        s.close()
        if port not in _used_ports:  # pragma: no branch
            _used_ports.add(port)
            return port


def wait_connection(
    host: str,
    port: int,
    *,
    sleep: float = 0.01,
    timeout: float | None = None,
) -> None:
    """Wait for a socket connection to be established.

    Repeatedly tries to open and close a socket connection to `host:port`.
    If successful, the function returns. If unsuccessful before the timeout,
    a `TimeoutError` is raised. The function will sleep for `sleep` seconds
    in between successive connection attempts.

    Args:
        host: Host address to connect to.
        port: Host port to connect to.
        sleep: Seconds to sleep after unsuccessful connections.
        timeout: Maximum number of seconds to wait for successful connections.
    """
    sleep = min(sleep, timeout) if timeout is not None else sleep
    waited = 0.0

    while True:
        try:
            start = time.perf_counter()
            with socket.create_connection((host, port), timeout=timeout):
                break
        except OSError as e:
            connection_time = time.perf_counter() - start
            waited += connection_time
            if timeout is not None and waited >= timeout:
                raise TimeoutError from e
            time.sleep(sleep)
            waited += sleep
