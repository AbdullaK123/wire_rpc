"""
TCP transports for Wire RPC.

TcpServerTransport — Server-side. Accepts one TCP connection
                      with 4-byte big-endian length-prefix framing.
TcpClientTransport — Client-side. Connects to a TCP server.

Pure asyncio. Zero dependencies beyond stdlib.
"""

import asyncio
from typing import Self
import uuid
import ssl
from wire_rpc.auth.protocol import Authenticator
from wire_rpc.logger import logger
from wire_rpc.transports.tcp._connection_limiter import (
    ConnectionLimiter,
    ConnectionLimitExceeded,
)
from wire_rpc.transports.tcp._tcp_connection import TcpConnection
from wire_rpc.transports.errors import IdleTimeoutError, InvalidFrameSizeError
from wire_rpc.transports.protocol import StartupComponent
from wire_rpc.transports.tcp.keep_alive import TcpKeepaliveConfig, configure_keepalive


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


class TcpServerTransport:

    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 9000,
        max_frame_size: int = 16 * 1024 * 1024,
        read_timeout: float = 30.0,
        auth_timeout: float = 10.0,
        write_timeout: float = 30.0,
        idle_timeout: float | None = 300.0,
        ssl_handshake_timeout: float = 10.0,
        ssl_shutdown_timeout: float = 10.0,
        ssl_context: ssl.SSLContext | None = None,
        keep_alive: TcpKeepaliveConfig | None = TcpKeepaliveConfig(),
        auth: Authenticator | None = None,
    ):
        self._host = host
        self._port = port
        self._max_frame_size = max_frame_size
        self._read_timeout = read_timeout
        self._auth_timeout = auth_timeout
        self._write_timeout = write_timeout
        self._idle_timeout = idle_timeout
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
        self._closing = False
        self._inflight: set[asyncio.Task[object]] = set()

    async def startup(self):
        if self._auth and isinstance(self._auth, StartupComponent):
            await self._auth.startup()

    async def shutdown(self):
        if self._auth and isinstance(self._auth, StartupComponent):
            await self._auth.shutdown()

    async def connect(self) -> None:
        self._server = await asyncio.start_server(
            self._handle_client, 
            self._host, 
            self._port,
            ssl=self._ssl,
            ssl_handshake_timeout=self._ssl_handshake_timeout,
            ssl_shutdown_timeout=self._ssl_shutdown_timeout
        )
        logger.info(f"TCP server listening on {self._host}:{self._port}")
        await self._connected.wait()

    async def _handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:

        if self._closing:
            raise ConnectionError("Transport is closing")

        if self._keep_alive is not None:
            configure_keepalive(
                writer,
                self._keep_alive
            )
        
        try:
            addr = None

            if self._auth:
                async with asyncio.timeout(self._auth_timeout):
                    addr = await self._auth.verify((reader, writer))

                if addr is None:
                    writer.close()
                    await writer.wait_closed()
                    return

            self._connection = TcpConnection(reader=reader, writer=writer)
            self._connected.set()
            logger.info(f"TCP client connected from {addr}")

        except asyncio.TimeoutError:
            logger.warning("Authentication timeout")
            writer.close()
            await writer.wait_closed()

    async def close(self) -> None:

        self._closing = True

        if self._inflight:
            await asyncio.gather(
                *self._inflight,
                return_exceptions=True,
            )

        if self._connection:
            await self._connection.close()
            self._connection = None
            self._connected.clear()

        if self._server:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

    async def recv(self) -> bytes:

        if self._closing:
            raise ConnectionError("Transport is closing")

        await self._connected.wait()

        connection = self._connection

        if connection is None:
            raise ConnectionError("No client connected")

        task = asyncio.current_task()

        if task is not None:
            self._inflight.add(task)

        try:
            return await _read_frame(
                connection,
                max_frame_size=self._max_frame_size,
                read_timeout=self._read_timeout,
                idle_timeout=self._idle_timeout,
            )
        except IdleTimeoutError:
            logger.error("Idle timeout")
            await connection.close()
            raise
        except asyncio.TimeoutError:
            logger.error("Read timeout")
            await connection.close()
            raise
        finally:
            if task is not None:
                self._inflight.discard(task)

    async def send(self, data: bytes) -> None:

        if self._closing:
            raise ConnectionError("Transport is closing")

        connection = self._connection

        if connection is None:
            raise ConnectionError("No client connected")
        
        if len(data) == 0 or len(data) > self._max_frame_size:
            raise InvalidFrameSizeError(self._max_frame_size)

        task = asyncio.current_task()

        if task is not None:
            self._inflight.add(task)

        try:
            _ensure_not_idle(connection, self._idle_timeout)

            async with connection.write_lock:
                connection.writer.write(len(data).to_bytes(4, "big"))
                connection.writer.write(data)
                async with asyncio.timeout(self._write_timeout):
                    await connection.writer.drain()
                connection.touch()
        except IdleTimeoutError:
            logger.error("Idle timeout")
            await connection.close()
            raise
        except asyncio.TimeoutError:
            logger.error("Write timeout")
            await connection.close()
            raise
        finally:
            if task is not None:
                self._inflight.discard(task)

    async def __aenter__(self) -> Self:
        self._connect_task = asyncio.create_task(self.connect())
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
        max_frame_size: int = 16 * 1024 * 1024,
        read_timeout: float = 30.0,
        write_timeout: float = 30.0,
        idle_timeout: float | None = 300.0,
        ssl_handshake_timeout: float = 10.0,
        ssl_shutdown_timeout: float = 10.0,
        ssl_context: ssl.SSLContext | None = None,
        keep_alive: TcpKeepaliveConfig | None = TcpKeepaliveConfig()
    ):
        self._host = host
        self._port = port
        self._read_timeout = read_timeout
        self._write_timeout = write_timeout
        self._idle_timeout = idle_timeout
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

    async def connect(self) -> None:

        reader, writer = await asyncio.open_connection(
            self._host, 
            self._port,
            ssl=self._ssl,
            ssl_handshake_timeout=self._ssl_handshake_timeout,
            ssl_shutdown_timeout=self._ssl_shutdown_timeout
        )
        
        if self._keep_alive is not None:
            configure_keepalive(
                writer,
                self._keep_alive
            )

        self._connection = TcpConnection(reader=reader, writer=writer)
        logger.info(f"Connected to TCP server at {self._host}:{self._port}")

    async def close(self) -> None:
        if self._connection:
            await self._connection.close()
            self._connection = None

    async def recv(self) -> bytes:
        connection = self._connection
        if connection is None:
            raise ConnectionError("Not connected")

        try:
            return await _read_frame(
                connection,
                max_frame_size=self._max_frame_size,
                read_timeout=self._read_timeout,
                idle_timeout=self._idle_timeout,
            )
        except IdleTimeoutError:
            logger.error("Idle timeout")
            await connection.close()
            raise
        except asyncio.TimeoutError:
            logger.error("Read timeout")
            await connection.close()
            raise

    async def send(self, data: bytes) -> None:
        connection = self._connection
        if connection is None:
            raise ConnectionError("Not connected")
        if len(data) == 0 or len(data) > self._max_frame_size:
            raise InvalidFrameSizeError(self._max_frame_size)

        try:
            _ensure_not_idle(connection, self._idle_timeout)

            async with connection.write_lock:
                connection.writer.write(len(data).to_bytes(4, "big"))
                connection.writer.write(data)
                async with asyncio.timeout(self._write_timeout):
                    await connection.writer.drain()
                connection.touch()
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
        max_frame_size: int = 16 * 1024 * 1024,
        read_timeout: float = 30.0,
        auth_timeout: float = 10.0,
        write_timeout: float = 30.0,
        idle_timeout: float | None = 300.0,
        max_connections: int = 1024,
        recv_queue_size: int = 1024,
        ssl_context: ssl.SSLContext | None = None,
        ssl_handshake_timeout: float = 10.0,
        ssl_shutdown_timeout: float = 10.0,
        keep_alive: TcpKeepaliveConfig | None = TcpKeepaliveConfig(),
        auth: Authenticator | None = None
    ):
        self._host = host
        self._port = port
        self._auth = auth
        self._read_timeout = read_timeout
        self._auth_timeout = auth_timeout
        self._write_timeout = write_timeout
        self._idle_timeout = idle_timeout
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
        self._recv_queue_size = recv_queue_size
        self._clients: dict[str, TcpConnection] = {}
        self._recv_queue: asyncio.Queue[tuple[str, bytes]] = asyncio.Queue(
            maxsize=self._recv_queue_size
        )
        self._server: asyncio.Server | None = None

    async def startup(self):
        if self._auth and isinstance(self._auth, StartupComponent):
            await self._auth.startup()

    async def shutdown(self):
        if self._auth and isinstance(self._auth, StartupComponent):
            await self._auth.shutdown()

    async def connect(self) -> None:
        self._server = await asyncio.start_server(
            self._handle_client, 
            self._host, 
            self._port,
            ssl=self._ssl,
            ssl_handshake_timeout=self._ssl_handshake_timeout,
            ssl_shutdown_timeout=self._ssl_shutdown_timeout
        )
        logger.info(
            f"TCP multicast server listening on {self._host}:{self._port}"
        )

    async def _handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        try:
            async with self._connection_limiter.slot():
                await self._serve_client(reader, writer)
        except ConnectionLimitExceeded:
            logger.warning("Max connections reached. Rejecting connection...")
            writer.close()
            await writer.wait_closed()

    async def _serve_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:

        
        if self._keep_alive is not None:
            configure_keepalive(
                writer,
                self._keep_alive
            )
        
        client_id = str(uuid.uuid4())[:8]
        addr = None
        connection = TcpConnection(reader=reader, writer=writer)

        try:
            if self._auth:
                try:
                    async with asyncio.timeout(self._auth_timeout):
                        addr = await self._auth.verify((reader, writer))
                except asyncio.TimeoutError:
                    logger.warning("Authentication timeout")
                    return

                if addr is None:
                    return

            self._clients[client_id] = connection
            logger.info(
                f"Client {client_id} connected from {addr} "
                f"({len(self._clients)} total)"
            )

            while True:
                payload = await _read_frame(
                    connection,
                    max_frame_size=self._max_frame_size,
                    read_timeout=self._read_timeout,
                    idle_timeout=self._idle_timeout,
                )
                await self._recv_queue.put((client_id, payload))

        except (asyncio.IncompleteReadError, ConnectionError, asyncio.CancelledError):
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
            self._clients.pop(client_id, None)
            await connection.close()
            logger.info(
                f"Client {client_id} disconnected "
                f"({len(self._clients)} total)"
            )

    async def close(self) -> None:
        connections = list(self._clients.values())
        self._clients.clear()

        for connection in connections:
            connection.writer.close()
        for connection in connections:
            await connection.writer.wait_closed()

        if self._server:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

    async def recv(self) -> tuple[str, bytes]:
        return await self._recv_queue.get()

    async def send(self, client_id: str, data: bytes) -> None:
        connection = self._clients.get(client_id)
        if connection is None:
            raise ConnectionError(f"Client {client_id} not connected")
        if len(data) == 0 or len(data) > self._max_frame_size:
            raise InvalidFrameSizeError(self._max_frame_size)

        try:
            _ensure_not_idle(connection, self._idle_timeout)

            async with connection.write_lock:
                connection.writer.write(len(data).to_bytes(4, "big"))
                connection.writer.write(data)
                async with asyncio.timeout(self._write_timeout):
                    await connection.writer.drain()
                connection.touch()
        except IdleTimeoutError:
            self._clients.pop(client_id, None)
            await connection.close()
            logger.warning(
                f"Client (id={client_id}) reached idle timeout. Disconnecting"
            )
            raise
        except asyncio.TimeoutError:
            logger.error(
                f"Client (id={client_id}) timed out on write. Disconnecting..."
            )
            self._clients.pop(client_id, None)
            await connection.close()
            logger.info(
                f"Client {client_id} disconnected "
                f"({len(self._clients)} total)"
            )
            raise

    async def broadcast(self, data: bytes) -> None:
        if len(data) == 0 or len(data) > self._max_frame_size:
            raise InvalidFrameSizeError(self._max_frame_size)

        frame = len(data).to_bytes(4, "big") + data
        dead: list[tuple[str, TcpConnection]] = []

        for client_id, connection in list(self._clients.items()):
            try:
                _ensure_not_idle(connection, self._idle_timeout)

                async with connection.write_lock:
                    connection.writer.write(frame)
                    async with asyncio.timeout(self._write_timeout):
                        await connection.writer.drain()
                    connection.touch()
            except (
                IdleTimeoutError,
                asyncio.TimeoutError,
                ConnectionError,
                ConnectionResetError,
            ):
                dead.append((client_id, connection))

        for client_id, connection in dead:
            self._clients.pop(client_id, None)
            await connection.close()

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



