"""
TCP transports for Wire RPC.

TcpServerTransport — Server-side. Accepts one TCP connection
                      with 4-byte big-endian length-prefix framing.
TcpClientTransport — Client-side. Connects to a TCP server.

Pure asyncio. Zero dependencies beyond stdlib.
"""

import asyncio
import time
from typing import Self
import uuid

from wire_rpc.auth.protocol import Authenticator
from wire_rpc.logger import logger
from wire_rpc.transports._connection_limiter import (
    ConnectionLimiter,
    ConnectionLimitExceeded,
)
from wire_rpc.transports._tcp_connection import TcpConnection
from wire_rpc.transports.errors import IdleTimeoutError, InvalidFrameSizeError
from wire_rpc.transports.protocol import StartupComponent


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
        auth: Authenticator | None = None,
    ):
        self._host = host
        self._port = port
        self._max_frame_size = max_frame_size
        self._read_timeout = read_timeout
        self._auth_timeout = auth_timeout
        self._writer_timeout = write_timeout
        self._idle_timeout = idle_timeout
        self._connected_at = time.monotonic()
        self._last_activity_at = time.monotonic()
        self._idle_for = 0.0
        self._auth: Authenticator | None = auth
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._server: asyncio.Server | None = None
        self._connected = asyncio.Event()
        self._writer_lock = asyncio.Lock()


    def _touch(self):
        self._last_activity_at = time.monotonic()
        self._idle_for = self._last_activity_at - self._connected_at

    async def startup(self):
        if self._auth and isinstance(self._auth, StartupComponent):
            await self._auth.startup()

    async def shutdown(self):
        if self._auth and isinstance(self._auth, StartupComponent):
            await self._auth.shutdown()

    async def connect(self) -> None:
        self._server = await asyncio.start_server(
            self._handle_client, self._host, self._port
        )
        self._connected_at = time.monotonic()
        logger.info(f"TCP server listening on {self._host}:{self._port}")
        await self._connected.wait()

    async def _handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ):
        try:
            addr = None

            if self._auth:
                async with asyncio.timeout(self._auth_timeout):
                    addr = await self._auth.verify((reader, writer))

                if addr is None:
                    writer.close()
                    await writer.wait_closed()
                    return

            logger.info(f"TCP client connected from {addr}")
            self._reader = reader
            self._writer = writer
            self._connected.set()

        except asyncio.TimeoutError:
            logger.warning("Authentication timeout")
            writer.close()
            await writer.wait_closed()
            return

    async def close(self) -> None:
        if self._writer:
            self._writer.close()
            await self._writer.wait_closed()
            self._writer = None
            self._reader = None
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

    async def recv(self) -> bytes:

        await self._connected.wait()


        try:
  
            if self._idle_timeout and self._idle_for > self._idle_timeout:
                raise IdleTimeoutError


            if self._reader is None:
                raise ConnectionError("No client connected")

            async with asyncio.timeout(self._read_timeout):
                length_bytes = await self._reader.readexactly(4)

            self._touch()

            length = int.from_bytes(length_bytes, "big")

            if length == 0 or length > self._max_frame_size:
                raise InvalidFrameSizeError(self._max_frame_size)

            async with asyncio.timeout(self._read_timeout):
                payload = await self._reader.readexactly(length)

            self._touch()

            return payload
        except IdleTimeoutError:
            logger.error("Idle timeout")
            if self._writer:
                self._writer.close()
                await self._writer.wait_closed()
            raise
        except asyncio.TimeoutError:
            logger.error("Read timeout")
            if self._writer:
                self._writer.close()
                await self._writer.wait_closed()
            raise

    async def send(self, data: bytes) -> None:

        if self._writer is None:
            raise ConnectionError("No client connected")
                
        if len(data) == 0 or len(data) > self._max_frame_size:
            raise InvalidFrameSizeError(self._max_frame_size)

        try:
            
            if self._idle_timeout and self._idle_for > self._idle_timeout:
                raise IdleTimeoutError

            async with self._writer_lock:
                self._writer.write(len(data).to_bytes(4, "big"))
                self._writer.write(data)
                async with asyncio.timeout(self._writer_timeout):
                    await self._writer.drain()
                self._touch()
        except IdleTimeoutError:
            logger.error("Idle timeout")
            if self._writer:
                self._writer.close()
                await self._writer.wait_closed()
            raise
        except asyncio.TimeoutError:
            logger.error("Write timeout")
            if self._writer:
                self._writer.close()
                await self._writer.wait_closed()
            raise

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
        idle_timeout: float = 30.0
    ):
        self._host = host
        self._port = port
        self._read_timeout = read_timeout
        self._write_timeout = write_timeout
        self._max_frame_size = max_frame_size
        self._idle_timeout = idle_timeout
        self._connected_at = time.monotonic()
        self._last_activity_at = time.monotonic()
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._writer_lock = asyncio.Lock()

    def _touch(self):
        self._last_activity_at = time.monotonic()
        self._idle_for = self._last_activity_at - self._connected_at

    async def connect(self) -> None:
        self._reader, self._writer = await asyncio.open_connection(
            self._host, self._port
        )
        self._connected_at = time.monotonic()
        logger.info(f"Connected to TCP server at {self._host}:{self._port}")

    async def close(self) -> None:
        if self._writer:
            self._writer.close()
            await self._writer.wait_closed()
            self._writer = None
            self._reader = None

    async def recv(self) -> bytes:
        try:
            if self._reader is None:
                raise ConnectionError("Not connected")
            if self._idle_timeout and self._idle_for > self._idle_timeout:
                raise IdleTimeoutError
            async with asyncio.timeout(self._read_timeout):
                length_bytes = await self._reader.readexactly(4)
            self._touch()
            length = int.from_bytes(length_bytes, "big")
            if length == 0 or length > self._max_frame_size:
                raise InvalidFrameSizeError(self._max_frame_size)
            async with asyncio.timeout(self._read_timeout):
                payload = await self._reader.readexactly(length)
            self._touch()
            return payload
        except IdleTimeoutError:
            logger.error("Idle timeout")
            if self._writer:
                self._writer.close()
                await self._writer.wait_closed()
            raise
        except asyncio.TimeoutError:
            logger.error("Read timeout")
            if self._writer:
                self._writer.close()
                await self._writer.wait_closed()
            raise

    async def send(self, data: bytes) -> None:
        if self._writer is None:
            raise ConnectionError("Not connected")
        if len(data) == 0 or len(data) > self._max_frame_size:
            raise InvalidFrameSizeError(self._max_frame_size)
        try:
            if self._idle_timeout and self._idle_for > self._idle_timeout:
                raise IdleTimeoutError
            async with self._writer_lock:
                self._writer.write(len(data).to_bytes(4, "big"))
                self._writer.write(data)
                async with asyncio.timeout(self._write_timeout):
                    await self._writer.drain()
                self._touch()
        except IdleTimeoutError:
            logger.error("Idle timeout")
            if self._writer:
                self._writer.close()
                await self._writer.wait_closed()
            raise
        except asyncio.TimeoutError:
            logger.error("Write timeout")
            if self._writer:
                self._writer.close()
                await self._writer.wait_closed()
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
        auth: Authenticator | None = None,
    ):
        self._host = host
        self._port = port
        self._auth = auth
        self._read_timeout = read_timeout
        self._auth_timeout = auth_timeout
        self._write_timeout = write_timeout
        self._idle_timeout = idle_timeout
        self._max_frame_size = max_frame_size
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
            self._handle_client, self._host, self._port
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

                if self._idle_timeout and connection.idle_for > self._idle_timeout:
                    raise IdleTimeoutError

                async with asyncio.timeout(self._read_timeout):
                    length_bytes = await connection.reader.readexactly(4)

                connection.touch()

                length = int.from_bytes(length_bytes, "big")
                if length == 0 or length > self._max_frame_size:
                    raise InvalidFrameSizeError(self._max_frame_size)

                async with asyncio.timeout(self._read_timeout):
                    payload = await connection.reader.readexactly(length)

                connection.touch()

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
            logger.warning(f"Client (id={client_id}) has reached idle timeout. Disconnecting")
        finally:
            self._clients.pop(client_id, None)
            connection.writer.close()
            await connection.writer.wait_closed()
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
            async with connection.write_lock:
                connection.writer.write(len(data).to_bytes(4, "big"))
                connection.writer.write(data)
                async with asyncio.timeout(self._write_timeout):
                    await connection.writer.drain()
                connection.touch()
        except asyncio.TimeoutError:
            logger.error(f"Client (id={client_id}) timed out on write. Disconnecting...")
            self._clients.pop(client_id, None)
            connection.writer.close()
            await connection.writer.wait_closed()
            logger.info(
                f"Client {client_id} disconnected "
                f"({len(self._clients)} total)"
            )
            

    async def broadcast(self, data: bytes) -> None:
        if len(data) == 0 or len(data) > self._max_frame_size:
            raise InvalidFrameSizeError(self._max_frame_size)

        frame = len(data).to_bytes(4, "big") + data
        dead: list[str] = []

        for client_id, connection in list(self._clients.items()):
            try:
                async with connection.write_lock:
                    connection.writer.write(frame)
                    try:
                        async with asyncio.timeout(self._write_timeout):
                            await connection.writer.drain()
                        connection.touch()
                    except asyncio.TimeoutError:
                        logger.error(f"Client (id={client_id}) timed out on write. Disconnecting...")
                        self._clients.pop(client_id, None)
                        connection.writer.close()
                        await connection.writer.wait_closed()
                        logger.info(
                            f"Client {client_id} disconnected "
                            f"({len(self._clients)} total)"
                        )
            except (ConnectionError, ConnectionResetError):
                dead.append(client_id)

        for client_id in dead:
            self._clients.pop(client_id, None)

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


__all__ = [
    "TcpMulticastServerTransport",
    "TcpServerTransport",
    "TcpClientTransport",
]
