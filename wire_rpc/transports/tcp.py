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
from wire_rpc.auth.protocol import Authenticator
from wire_rpc.logger import logger


class TcpServerTransport:

    def __init__(
        self, 
        host: str = "0.0.0.0", 
        port: int = 9000,
        auth: Authenticator | None = None
    ):
        self._host = host
        self._port = port
        self._auth: Authenticator | None = auth
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._server: asyncio.Server | None = None
        self._connected = asyncio.Event()

    async def connect(self) -> None:
        self._server = await asyncio.start_server(
            self._handle_client, self._host, self._port
        )
        logger.info(f"TCP server listening on {self._host}:{self._port}")
        await self._connected.wait()

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):

        addr = None

        if self._auth:
            addr = await self._auth.verify((reader, writer))
            if addr is None:
                writer.close()
                return
            
        logger.info(f"TCP client connected from {addr}")
        self._reader = reader
        self._writer = writer
        self._connected.set()

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
        if self._reader is None:
            raise ConnectionError("No client connected")
        length_bytes = await self._reader.readexactly(4)
        length = int.from_bytes(length_bytes, "big")
        return await self._reader.readexactly(length)

    async def send(self, data: bytes) -> None:
        if self._writer is None:
            raise ConnectionError("No client connected")
        self._writer.write(len(data).to_bytes(4, "big"))
        self._writer.write(data)
        await self._writer.drain()

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

    def __init__(self, host: str = "localhost", port: int = 9000):
        self._host = host
        self._port = port
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None

    async def connect(self) -> None:
        self._reader, self._writer = await asyncio.open_connection(
            self._host, self._port
        )
        logger.info(f"Connected to TCP server at {self._host}:{self._port}")

    async def close(self) -> None:
        if self._writer:
            self._writer.close()
            await self._writer.wait_closed()
            self._writer = None
            self._reader = None

    async def recv(self) -> bytes:
        if self._reader is None:
            raise ConnectionError("Not connected")
        length_bytes = await self._reader.readexactly(4)
        length = int.from_bytes(length_bytes, "big")
        return await self._reader.readexactly(length)

    async def send(self, data: bytes) -> None:
        if self._writer is None:
            raise ConnectionError("Not connected")
        self._writer.write(len(data).to_bytes(4, "big"))
        self._writer.write(data)
        await self._writer.drain()

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
        auth: Authenticator | None = None
    ):
        self._host = host
        self._port = port
        self._auth = auth
        self._clients: dict[str, tuple[asyncio.StreamReader, asyncio.StreamWriter]] = {}
        self._recv_queue: asyncio.Queue[tuple[str, bytes]] = asyncio.Queue()
        self._server: asyncio.Server | None = None

    async def connect(self) -> None:
        self._server = await asyncio.start_server(
            self._handle_client, self._host, self._port
        )
        logger.info(f"TCP multicast server listening on {self._host}:{self._port}")

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):

        client_id = str(uuid.uuid4())[:8]
        addr = None

        if self._auth:
            addr = await self._auth.verify((reader, writer))
            if addr is None:
                writer.close()
                return 
            
        self._clients[client_id] = (reader, writer)
        logger.info(f"Client {client_id} connected from {addr} ({len(self._clients)} total)")

        try:
            while True:
                length_bytes = await reader.readexactly(4)
                length = int.from_bytes(length_bytes, "big")
                payload = await reader.readexactly(length)
                self._recv_queue.put_nowait((client_id, payload))
        except (asyncio.IncompleteReadError, ConnectionError, asyncio.CancelledError):
            pass
        finally:
            self._clients.pop(client_id, None)
            writer.close()
            logger.info(f"Client {client_id} disconnected ({len(self._clients)} total)")

    async def close(self) -> None:
        for client_id, (reader, writer) in list(self._clients.items()):
            writer.close()
        self._clients.clear()
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

    async def recv(self) -> tuple[str, bytes]:
        return await self._recv_queue.get()

    async def send(self, client_id: str, data: bytes) -> None:
        pair = self._clients.get(client_id)
        if pair is None:
            raise ConnectionError(f"Client {client_id} not connected")
        reader, writer = pair
        writer.write(len(data).to_bytes(4, "big"))
        writer.write(data)
        await writer.drain()

    async def broadcast(self, data: bytes) -> None:
        frame = len(data).to_bytes(4, "big") + data
        dead: list[str] = []
        for client_id, (reader, writer) in self._clients.items():
            try:
                writer.write(frame)
                await writer.drain()
            except (ConnectionError, ConnectionResetError):
                dead.append(client_id)
        for client_id in dead:
            del self._clients[client_id]

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

__all__ =  [
    "TcpMulticastServerTransport",
    "TcpServerTransport",
    "TcpClientTransport"
]