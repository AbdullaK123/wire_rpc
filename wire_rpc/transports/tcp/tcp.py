"""
TCP transports for Wire RPC.

TcpServerTransport — Server-side. Accepts one TCP connection
                      with 4-byte big-endian length-prefix framing.
TcpClientTransport — Client-side. Connects to a TCP server.

Pure asyncio. Zero dependencies beyond stdlib.
"""

import asyncio
import ssl
import uuid
from wire_rpc._validation import positive_timeout, positive_limit
from wire_rpc.transports._queue import PayloadQueue
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Generator, Self

from wire_rpc.auth.protocol import Authenticator
from wire_rpc.logger import logger
from wire_rpc.transports.errors import IdleTimeoutError, InvalidFrameSizeError
from wire_rpc.transports.protocol import StartupComponent
from wire_rpc.transports.tcp._connection_limiter import (
    ConnectionLimiter,
    ConnectionLimitExceeded,
)
from wire_rpc.transports.tcp._tcp_connection import TcpConnection
from wire_rpc.transports.tcp.keep_alive import (
    TcpKeepaliveConfig,
    configure_keepalive,
)


async def _read_frame(
    connection: TcpConnection,
    *,
    max_frame_size: int,
    read_timeout: float,
    idle_timeout: float | None,
) -> bytes:
    if idle_timeout is None:
        first_header_byte = await connection.reader.readexactly(1)
    else:
        remaining_idle = idle_timeout - connection.idle_for
        if remaining_idle <= 0:
            raise IdleTimeoutError

        try:
            async with asyncio.timeout(remaining_idle):
                first_header_byte = await connection.reader.readexactly(1)
        except asyncio.TimeoutError as exc:
            raise IdleTimeoutError from exc

    async with asyncio.timeout(read_timeout):
        remaining_header = await connection.reader.readexactly(3)
        length = int.from_bytes(first_header_byte + remaining_header, "big")

        if length == 0 or length > max_frame_size:
            raise InvalidFrameSizeError(max_frame_size)

        payload = await connection.reader.readexactly(length)

    connection.touch()
    return payload


def _ensure_not_idle(
    connection: TcpConnection,
    idle_timeout: float | None,
) -> None:
    if idle_timeout is not None and connection.idle_for >= idle_timeout:
        raise IdleTimeoutError


@contextmanager
def _track_operation(
    inflight: set[asyncio.Task[object]],
    closing: bool,
) -> Generator[None, None, None]:
    if closing:
        raise ConnectionError("Transport is closing")

    task = asyncio.current_task()
    if task is not None:
        inflight.add(task)

    try:
        yield
    finally:
        if task is not None:
            inflight.discard(task)


async def _drain_tasks(
    tasks: set[asyncio.Task[object]],
    timeout: float,
) -> None:
    current = asyncio.current_task()
    tracked = {
        task
        for task in tasks
        if task is not current and not task.done()
    }

    if not tracked:
        return

    _, pending = await asyncio.wait(tracked, timeout=timeout)

    if pending:
        logger.warning(
            f"Graceful shutdown timed out; cancelling {len(pending)} task(s)"
        )
        for task in pending:
            task.cancel()

    await asyncio.gather(*tracked, return_exceptions=True)


async def _close_writer(writer: asyncio.StreamWriter) -> None:
    writer.close()
    try:
        async with asyncio.timeout(5.0):
            await writer.wait_closed()
    except (ConnectionError, OSError, TimeoutError):
        transport = getattr(writer, "transport", None)
        if transport is not None:
            transport.abort()


class TcpServerTransport:

    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 9000,
        max_frame_size: int = 1024 * 1024,
        read_timeout: float = 30.0,
        auth_timeout: float = 10.0,
        write_timeout: float = 30.0,
        idle_timeout: float | None = 300.0,
        ssl_handshake_timeout: float = 10.0,
        ssl_shutdown_timeout: float = 10.0,
        ssl_context: ssl.SSLContext | None = None,
        keep_alive: TcpKeepaliveConfig | None = TcpKeepaliveConfig(),
        auth: Authenticator | None = None,
        shutdown_timeout: float = 30.0,
    ):
        positive_limit('max_frame_size', max_frame_size)
        for name, value in [('read_timeout',read_timeout),('write_timeout',write_timeout),
                            ('shutdown_timeout',shutdown_timeout),('ssl_handshake_timeout',ssl_handshake_timeout),
                            ('ssl_shutdown_timeout',ssl_shutdown_timeout)]:
            positive_timeout(name, value)
        if idle_timeout is not None:
            positive_timeout('idle_timeout', idle_timeout)
        if not 0 < max_frame_size <= 0xFFFFFFFF:
            raise ValueError("Frame size must fit a positive 4-byte length")
        if shutdown_timeout <= 0:
            raise ValueError("Shutdown timeout must be greater than zero")

        self._host = host
        self._port = port
        self._max_frame_size = max_frame_size
        self._read_timeout = read_timeout
        positive_timeout("auth_timeout", auth_timeout)
        self._auth_timeout = auth_timeout
        self._write_timeout = write_timeout
        self._idle_timeout = idle_timeout
        self._shutdown_timeout = shutdown_timeout
        self._ssl_handshake_timeout = (
            ssl_handshake_timeout if ssl_context is not None else None
        )
        self._ssl_shutdown_timeout = (
            ssl_shutdown_timeout if ssl_context is not None else None
        )
        self._ssl = ssl_context
        self._auth: Authenticator | None = auth
        self._connection: TcpConnection | None = None
        self._server: asyncio.Server | None = None
        self._keep_alive = keep_alive
        self._connected = asyncio.Event()
        self._accepting = False
        self._closing = False
        self._inflight: set[asyncio.Task[object]] = set()
        self._connection_tasks: set[asyncio.Task[object]] = set()
        self._connect_task: asyncio.Task[None] | None = None

    async def startup(self):
        if self._auth and isinstance(self._auth, StartupComponent):
            await self._auth.startup()

    async def shutdown(self):
        if self._auth and isinstance(self._auth, StartupComponent):
            await self._auth.shutdown()

    async def _start(self) -> None:
        with _track_operation(self._inflight, self._closing):
            server = await asyncio.start_server(
                self._handle_client,
                self._host,
                self._port,
                ssl=self._ssl,
                ssl_handshake_timeout=self._ssl_handshake_timeout,
                ssl_shutdown_timeout=self._ssl_shutdown_timeout,
            )

            if self._closing:
                server.close()
                await server.wait_closed()
                raise ConnectionError("Transport is closing")

            self._server = server
            logger.info(f"TCP server listening on {self._host}:{self._port}")

    async def connect(self) -> None:
        await self._start()
        await self._connected.wait()
        if self._closing:
            raise ConnectionError('Transport is closing')

    async def _handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        task = asyncio.current_task()
        if task is not None:
            self._connection_tasks.add(task)

        transferred = False
        reserved = False

        try:
            if self._closing or self._accepting or self._connection is not None:
                return
            self._accepting = True
            reserved = True

            if self._keep_alive is not None:
                configure_keepalive(writer, self._keep_alive)

            addr = None

            if self._auth:
                async with asyncio.timeout(self._auth_timeout):
                    addr = await self._auth.verify((reader, writer))

                if addr is None:
                    return

            if self._closing:
                return

            self._connection = TcpConnection(reader=reader, writer=writer, principal=addr)
            transferred = True
            self._connected.set()
            logger.info(f"TCP client connected from {addr}")

        except asyncio.TimeoutError:
            logger.warning("Authentication timeout")
        finally:
            if reserved:
                self._accepting = False
            if not transferred:
                await _close_writer(writer)
            if task is not None:
                self._connection_tasks.discard(task)

    async def close(self) -> None:
        self._closing = True

        if self._server:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

        # Wake connect()/recv() calls waiting for the first connection. They
        # re-check _closing before doing any more work.
        self._connected.set()

        await _drain_tasks(
            self._inflight | self._connection_tasks,
            self._shutdown_timeout,
        )

        if self._connect_task is not None:
            if self._connect_task is not asyncio.current_task():
                await asyncio.gather(
                    self._connect_task,
                    return_exceptions=True,
                )
            self._connect_task = None

        if self._connection:
            await self._connection.close()
            self._connection = None

        self._connected.clear()

    @property
    def max_message_size(self):
        return self._max_frame_size

    async def get_principal(self, client_id=None):
        connection = self._connection
        if connection is None or connection.writer.is_closing():
            raise PermissionError('Peer disconnected')
        return connection.principal

    async def recv(self) -> bytes:
        with _track_operation(self._inflight, self._closing):
            await self._connected.wait()

            if self._closing:
                raise ConnectionError("Transport is closing")

            connection = self._connection
            if connection is None:
                raise ConnectionError("No client connected")

            async with connection.read_lock:
                if connection.writer.is_closing():
                    raise ConnectionError("Connection closed")
                try:
                    return await _read_frame(
                        connection,
                        max_frame_size=self._max_frame_size,
                        read_timeout=self._read_timeout,
                        idle_timeout=self._idle_timeout,
                    )
                except BaseException:
                    # Partial reads cannot be resumed at a known frame boundary.
                    await connection.close()
                    raise

    async def send(self, data: bytes) -> None:
        with _track_operation(self._inflight, self._closing):
            connection = self._connection
            if connection is None:
                raise ConnectionError("No client connected")

            if len(data) == 0 or len(data) > self._max_frame_size:
                raise InvalidFrameSizeError(self._max_frame_size)

            try:
                _ensure_not_idle(connection, self._idle_timeout)

                async with connection.write_lock:
                    if connection.writer.is_closing():
                        raise ConnectionError("Connection closed")
                    connection.writer.write(len(data).to_bytes(4, "big"))
                    connection.writer.write(data)
                    async with asyncio.timeout(self._write_timeout):
                        await connection.writer.drain()
                    connection.touch()
            except (asyncio.CancelledError, ConnectionError, OSError):
                await connection.close()
                raise
            except IdleTimeoutError:
                logger.error("Idle timeout")
                await connection.close()
                raise
            except asyncio.TimeoutError:
                logger.error("Write timeout")
                await connection.close()
                raise

    async def __aenter__(self) -> Self:
        await self._start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        await self.close()


class TcpClientTransport:

    def __init__(
        self,
        host: str = "localhost",
        port: int = 9000,
        max_frame_size: int = 1024 * 1024,
        read_timeout: float = 30.0,
        write_timeout: float = 30.0,
        idle_timeout: float | None = 300.0,
        ssl_handshake_timeout: float = 10.0,
        ssl_shutdown_timeout: float = 10.0,
        ssl_context: ssl.SSLContext | None = None,
        keep_alive: TcpKeepaliveConfig | None = TcpKeepaliveConfig(),
        shutdown_timeout: float = 30.0,
        *,
        connect_timeout: float = 10.0,
        auth=None,
    ):
        positive_limit('max_frame_size', max_frame_size)
        for name, value in [('read_timeout',read_timeout),('write_timeout',write_timeout),
                            ('shutdown_timeout',shutdown_timeout),('ssl_handshake_timeout',ssl_handshake_timeout),
                            ('ssl_shutdown_timeout',ssl_shutdown_timeout)]:
            positive_timeout(name, value)
        if idle_timeout is not None:
            positive_timeout('idle_timeout', idle_timeout)
        if not 0 < max_frame_size <= 0xFFFFFFFF:
            raise ValueError("Frame size must fit a positive 4-byte length")
        if shutdown_timeout <= 0:
            raise ValueError("Shutdown timeout must be greater than zero")

        positive_timeout("connect_timeout", connect_timeout)
        self._connect_timeout = connect_timeout
        self._client_auth = auth
        self._connecting = False
        self._host = host
        self._port = port
        self._read_timeout = read_timeout
        self._write_timeout = write_timeout
        self._idle_timeout = idle_timeout
        self._shutdown_timeout = shutdown_timeout
        self._max_frame_size = max_frame_size
        self._ssl = ssl_context
        self._keep_alive = keep_alive
        self._ssl_handshake_timeout = (
            ssl_handshake_timeout if ssl_context is not None else None
        )
        self._ssl_shutdown_timeout = (
            ssl_shutdown_timeout if ssl_context is not None else None
        )
        self._connection: TcpConnection | None = None
        self._closing = False
        self._inflight: set[asyncio.Task[object]] = set()

    async def connect(self) -> None:
        if self._connecting or self._connection is not None:
            raise RuntimeError('Connection already exists or is in progress')
        self._connecting = True
        writer = None
        try:
            with _track_operation(self._inflight, self._closing):
                async with asyncio.timeout(self._connect_timeout):
                    reader, writer = await asyncio.open_connection(
                        self._host, self._port, ssl=self._ssl,
                        ssl_handshake_timeout=self._ssl_handshake_timeout,
                        ssl_shutdown_timeout=self._ssl_shutdown_timeout)
                    if self._keep_alive is not None:
                        configure_keepalive(writer, self._keep_alive)
                    if self._client_auth is not None:
                        await self._client_auth.authenticate((reader,writer))
                    if self._closing:
                        raise ConnectionError('Transport closing')
                    self._connection = TcpConnection(reader=reader, writer=writer)
        except BaseException:
            if writer is not None:
                await _close_writer(writer)
            raise
        finally:
            self._connecting = False

    async def close(self) -> None:
        self._closing = True

        await _drain_tasks(
            self._inflight,
            self._shutdown_timeout,
        )

        if self._connection:
            await self._connection.close()
            self._connection = None

    @property
    def max_message_size(self):
        return self._max_frame_size

    async def get_principal(self, client_id=None):
        connection = self._connection
        if connection is None or connection.writer.is_closing():
            raise PermissionError('Peer disconnected')
        return connection.principal

    async def recv(self) -> bytes:
        with _track_operation(self._inflight, self._closing):
            connection = self._connection
            if connection is None:
                raise ConnectionError("Not connected")

            async with connection.read_lock:
                if connection.writer.is_closing():
                    raise ConnectionError("Connection closed")
                try:
                    return await _read_frame(
                        connection,
                        max_frame_size=self._max_frame_size,
                        read_timeout=self._read_timeout,
                        idle_timeout=self._idle_timeout,
                    )
                except BaseException:
                    # Partial reads cannot be resumed at a known frame boundary.
                    await connection.close()
                    raise

    async def send(self, data: bytes) -> None:
        with _track_operation(self._inflight, self._closing):
            connection = self._connection
            if connection is None:
                raise ConnectionError("Not connected")

            if len(data) == 0 or len(data) > self._max_frame_size:
                raise InvalidFrameSizeError(self._max_frame_size)

            try:
                _ensure_not_idle(connection, self._idle_timeout)

                async with connection.write_lock:
                    if connection.writer.is_closing():
                        raise ConnectionError("Connection closed")
                    connection.writer.write(len(data).to_bytes(4, "big"))
                    connection.writer.write(data)
                    async with asyncio.timeout(self._write_timeout):
                        await connection.writer.drain()
                    connection.touch()
            except (asyncio.CancelledError, ConnectionError, OSError):
                await connection.close()
                raise
            except IdleTimeoutError:
                logger.error("Idle timeout")
                await connection.close()
                raise
            except asyncio.TimeoutError:
                logger.error("Write timeout")
                await connection.close()
                raise

    async def __aenter__(self) -> Self:
        await self.connect()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        await self.close()


class TcpMulticastServerTransport:

    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 9000,
        max_frame_size: int = 1024 * 1024,
        read_timeout: float = 30.0,
        auth_timeout: float = 10.0,
        write_timeout: float = 30.0,
        idle_timeout: float | None = 300.0,
        max_connections: int = 64,
        recv_queue_size: int = 256,
        ssl_context: ssl.SSLContext | None = None,
        ssl_handshake_timeout: float = 10.0,
        ssl_shutdown_timeout: float = 10.0,
        keep_alive: TcpKeepaliveConfig | None = TcpKeepaliveConfig(),
        auth: Authenticator | None = None,
        shutdown_timeout: float = 30.0,
        *,
        max_queue_bytes: int = 16 * 1024 * 1024,
        per_peer_queue_size: int = 16,
    ):
        positive_limit('max_frame_size', max_frame_size)
        for name, value in [('read_timeout',read_timeout),('write_timeout',write_timeout),
                            ('shutdown_timeout',shutdown_timeout),('ssl_handshake_timeout',ssl_handshake_timeout),
                            ('ssl_shutdown_timeout',ssl_shutdown_timeout)]:
            positive_timeout(name, value)
        if idle_timeout is not None:
            positive_timeout('idle_timeout', idle_timeout)
        if not 0 < max_frame_size <= 0xFFFFFFFF:
            raise ValueError("Frame size must fit a positive 4-byte length")
        if shutdown_timeout <= 0:
            raise ValueError("Shutdown timeout must be greater than zero")

        self._host = host
        self._port = port
        self._auth = auth
        self._read_timeout = read_timeout
        positive_timeout("auth_timeout", auth_timeout)
        self._auth_timeout = auth_timeout
        self._write_timeout = write_timeout
        self._idle_timeout = idle_timeout
        self._shutdown_timeout = shutdown_timeout
        self._max_frame_size = max_frame_size
        self._keep_alive = keep_alive
        self._ssl = ssl_context
        self._ssl_handshake_timeout = (
            ssl_handshake_timeout if ssl_context is not None else None
        )
        self._ssl_shutdown_timeout = (
            ssl_shutdown_timeout if ssl_context is not None else None
        )
        self._connection_limiter = ConnectionLimiter(max_connections)
        if recv_queue_size <= 0:
            raise ValueError("Receive queue size must be positive")
        self._recv_queue_size = recv_queue_size
        self._clients: dict[str, TcpConnection] = {}
        if max_frame_size > max_queue_bytes:
            raise ValueError('A frame must fit the queue byte budget')
        self._recv_queue = PayloadQueue(recv_queue_size, max_bytes=max_queue_bytes, per_peer=per_peer_queue_size)
        self._server: asyncio.Server | None = None
        self._closing = False
        self._inflight: set[asyncio.Task[object]] = set()
        self._connection_tasks: set[asyncio.Task[object]] = set()

    async def startup(self):
        if self._auth and isinstance(self._auth, StartupComponent):
            await self._auth.startup()

    async def shutdown(self):
        if self._auth and isinstance(self._auth, StartupComponent):
            await self._auth.shutdown()

    async def connect(self) -> None:
        with _track_operation(self._inflight, self._closing):
            server = await asyncio.start_server(
                self._handle_client,
                self._host,
                self._port,
                ssl=self._ssl,
                ssl_handshake_timeout=self._ssl_handshake_timeout,
                ssl_shutdown_timeout=self._ssl_shutdown_timeout,
            )

            if self._closing:
                server.close()
                await server.wait_closed()
                raise ConnectionError("Transport is closing")

            self._server = server
            logger.info(
                f"TCP multicast server listening on {self._host}:{self._port}"
            )

    async def _handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        task = asyncio.current_task()
        if task is not None:
            self._connection_tasks.add(task)

        try:
            if self._closing:
                await _close_writer(writer)
                return

            try:
                async with self._connection_limiter.slot():
                    if self._closing:
                        await _close_writer(writer)
                        return
                    await self._serve_client(reader, writer)
            except ConnectionLimitExceeded:
                logger.warning(
                    "Max connections reached. Rejecting connection..."
                )
                await _close_writer(writer)
        finally:
            if task is not None:
                self._connection_tasks.discard(task)

    async def _serve_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        client_id = str(uuid.uuid4())
        addr = None
        connection = TcpConnection(reader=reader, writer=writer)
        registered = False

        try:
            if self._keep_alive is not None:
                configure_keepalive(writer, self._keep_alive)

            if self._closing:
                return

            if self._auth:
                try:
                    async with asyncio.timeout(self._auth_timeout):
                        addr = await self._auth.verify((reader, writer))
                except asyncio.TimeoutError:
                    logger.warning("Authentication timeout")
                    return

                if addr is None:
                    return

            if self._closing:
                return

            connection.principal = addr
            self._clients[client_id] = connection
            registered = True
            logger.info(
                f"Client {client_id} connected from {addr} "
                f"({len(self._clients)} total)"
            )

            while not self._closing:
                payload = await _read_frame(
                    connection,
                    max_frame_size=self._max_frame_size,
                    read_timeout=self._read_timeout,
                    idle_timeout=self._idle_timeout,
                )

                if self._closing:
                    break

                await self._recv_queue.put((client_id, payload))

        except (
            asyncio.IncompleteReadError,
            ConnectionError,
            asyncio.CancelledError,
        ):
            pass
        except InvalidFrameSizeError:
            logger.warning(
                f"Client {client_id} sent an invalid frame size"
            )
        except asyncio.TimeoutError:
            logger.warning(f"Read timeout from {client_id}")
        except IdleTimeoutError:
            logger.warning(
                f"Client (id={client_id}) reached idle timeout. Disconnecting"
            )
        finally:
            if self._closing and registered:
                # close() owns registered connections during shutdown so an
                # in-flight send cannot race this reader task closing them.
                pass
            else:
                if registered:
                    self._clients.pop(client_id, None)
                    self._recv_queue.drop_peer(client_id)

                await connection.close()

                if registered:
                    logger.info(
                        f"Client {client_id} disconnected "
                        f"({len(self._clients)} total)"
                    )

    async def close(self) -> None:
        self._closing = True
        self._recv_queue.close()

        if self._server:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

        await _drain_tasks(
            self._inflight | self._connection_tasks,
            self._shutdown_timeout,
        )

        connections = list(self._clients.values())
        self._clients.clear()

        if connections:
            await asyncio.gather(
                *(connection.close() for connection in connections),
                return_exceptions=True,
            )

    @property
    def max_message_size(self):
        return self._max_frame_size

    @property
    def stats(self):
        return {**self._recv_queue.stats, 'connections': len(self._clients)}

    async def get_principal(self, client_id=None):
        connection = self._clients.get(client_id)
        if connection is None or connection.writer.is_closing():
            raise PermissionError('Peer disconnected')
        return connection.principal

    async def recv(self) -> tuple[str, bytes]:
        with _track_operation(self._inflight, self._closing):
            return await self._recv_queue.get()

    async def send(self, client_id: str, data: bytes) -> None:
        with _track_operation(self._inflight, self._closing):
            connection = self._clients.get(client_id)
            if connection is None:
                raise ConnectionError(f"Client {client_id} not connected")

            if len(data) == 0 or len(data) > self._max_frame_size:
                raise InvalidFrameSizeError(self._max_frame_size)

            try:
                _ensure_not_idle(connection, self._idle_timeout)

                async with connection.write_lock:
                    if connection.writer.is_closing():
                        raise ConnectionError("Connection closed")
                    connection.writer.write(len(data).to_bytes(4, "big"))
                    connection.writer.write(data)
                    async with asyncio.timeout(self._write_timeout):
                        await connection.writer.drain()
                    connection.touch()
            except (asyncio.CancelledError, ConnectionError, OSError):
                if self._clients.get(client_id) is connection:
                    self._clients.pop(client_id, None)
                    self._recv_queue.drop_peer(client_id)
                await connection.close()
                raise
            except IdleTimeoutError:
                self._clients.pop(client_id, None)
                await connection.close()
                logger.warning(
                    f"Client (id={client_id}) reached idle timeout. "
                    "Disconnecting"
                )
                raise
            except asyncio.TimeoutError:
                logger.error(
                    f"Client (id={client_id}) timed out on write. "
                    "Disconnecting..."
                )
                self._clients.pop(client_id, None)
                await connection.close()
                logger.info(
                    f"Client {client_id} disconnected "
                    f"({len(self._clients)} total)"
                )
                raise

    async def broadcast(self, data: bytes) -> None:
        with _track_operation(self._inflight, self._closing):
            if len(data) == 0 or len(data) > self._max_frame_size:
                raise InvalidFrameSizeError(self._max_frame_size)
            async def deliver(client_id):
                try:
                    await self.send(client_id, data)
                except (ConnectionError, OSError, IdleTimeoutError):
                    pass
            # Fanout is bounded by connection admission, not one unbounded task
            # per message. Slow recipients do not serialize unrelated peers.
            await asyncio.gather(*(deliver(key) for key in list(self._clients)))

    async def __aenter__(self) -> Self:
        await self.connect()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        await self.close()
