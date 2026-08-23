"""
WebSocket transports for Wire RPC.

WsServerTransport — Server-side. Runs an aiohttp server accepting
                     one WebSocket connection. Optionally serves
                     static files for a self-contained web app.
WsClientTransport — Client-side. Connects to a WebSocket endpoint.

Both use binary WebSocket frames — no length-prefix needed,
WebSocket handles message framing natively.
"""

import asyncio
from pathlib import Path
from typing import Self
import uuid
import aiohttp
from aiohttp import web
from wire_rpc.auth.protocol import Authenticator
from wire_rpc.logger import logger


class WsServerTransport:

    def __init__(
        self, 
        host: str = "0.0.0.0", 
        port: int = 8000, 
        static_dir: str | None = None,
        auth: Authenticator | None = None
    ):
        self._host = host
        self._port = port
        self._static_dir = static_dir
        self._auth = auth
        self._ws: web.WebSocketResponse | None = None
        self._runner: web.AppRunner | None = None
        self._recv_queue: asyncio.Queue[bytes] = asyncio.Queue()
        self._connected = asyncio.Event()

    async def connect(self):
        app = web.Application()
        app.router.add_get("/ws", self._handle_ws)

        if self._auth:
            app.router.add_post("/login", self._auth.login)

        if self._static_dir:
            # Serve index.html on /
            static_path = Path(self._static_dir)
            async def serve_index(request):
                return web.FileResponse(static_path / "index.html")
            app.router.add_get("/", serve_index)
            app.router.add_static("/static", static_path)

        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, self._host, self._port)
        await site.start()
        logger.info(f"Wire WS server running at http://{self._host}:{self._port}")
        await self._connected.wait()

    async def _handle_ws(self, request: web.Request) -> web.WebSocketResponse:

        if self._auth:
            user_id = await self._auth.verify(request)
            if user_id is None:
                raise web.HTTPUnauthorized(text="Invalid credentials")

        ws = web.WebSocketResponse()
        await ws.prepare(request)
        self._ws = ws
        self._connected.set()
        logger.info("WebSocket client connected")

        try:
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.BINARY:
                    self._recv_queue.put_nowait(msg.data)
                elif msg.type == aiohttp.WSMsgType.TEXT:
                    self._recv_queue.put_nowait(msg.data.encode())
        finally:
            self._ws = None
            logger.info("WebSocket client disconnected")

        return ws

    async def close(self):
        if self._ws:
            await self._ws.close()
            self._ws = None
        if self._runner:
            await self._runner.cleanup()
            self._runner = None

    async def recv(self) -> bytes:
        return await self._recv_queue.get()

    async def send(self, data: bytes) -> None:
        if self._ws is None:
            raise ConnectionError("No WebSocket client connected")
        await self._ws.send_str(data.decode())

    async def __aenter__(self) -> Self:
        # Don't await connect — run it as a background task
        # so the listen loop can start while waiting for a client
        self._connect_task = asyncio.create_task(self.connect())
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object
    ) -> None:
        await self.close()


class MulticastWsServerTransport:

    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 8000,
        static_dir: str | None = None,
        auth: Authenticator | None = None
    ):
        self._host = host
        self._port = port
        self._static_dir = static_dir
        self._clients: dict[str, web.WebSocketResponse] = {}
        self._runner: web.AppRunner | None = None
        self._recv_queue: asyncio.Queue[tuple[str, bytes]] = asyncio.Queue()
        self._auth = auth

    async def connect(self):

        app = web.Application()
        app.router.add_get("/ws", self._handle_ws)

        if self._auth:
            app.router.add_post("/login", self._auth.login)
            app.router.add_post("/logout", self._auth.logout)

        if self._static_dir:
            static_path = Path(self._static_dir)

            async def serve_index(request: web.Request) -> web.FileResponse:
                return web.FileResponse(static_path / "index.html")

            app.router.add_get("/", serve_index)
            app.router.add_static("/static", static_path)

        self._runner = web.AppRunner(app)
        await self._runner.setup()

        site = web.TCPSite(self._runner, self._host, self._port)

        await site.start()
        logger.info(f"Multicast WS server running at http://{self._host}:{self._port}")


    async def _handle_ws(self, request: web.Request) -> web.WebSocketResponse:

        client_id = str(uuid.uuid4())[:8]

        if self._auth:
            user_id = await self._auth.verify(request)
            if user_id is None:
                raise web.HTTPUnauthorized(text="Invalid credentials")
            client_id = user_id

        ws = web.WebSocketResponse()
        await ws.prepare(request)   
     
        self._clients[client_id] = ws

        logger.info(f"Client {client_id} connected ({len(self._clients)} total)")

        try:
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.BINARY:
                    self._recv_queue.put_nowait((client_id, msg.data))
                if msg.type == aiohttp.WSMsgType.TEXT:
                    self._recv_queue.put_nowait((client_id, msg.data.encode()))
        finally:
            del self._clients[client_id]
            logger.info(f"Client {client_id} disconnected ({len(self._clients)} total)")

        return ws

    async def recv(self) -> tuple[str, bytes]:
        return await self._recv_queue.get()

    async def send(self, client_id: str, data: bytes):

        ws = self._clients.get(client_id)

        if ws is None:
            raise ConnectionError(f"Client (id={client_id}) not connected")

        await ws.send_str(data.decode())

    async def broadcast(self, data: bytes):

        msg = data.decode()
        dead: list[str] = []

        for client_id, ws in self._clients.items():
            try:
                await ws.send_str(msg)
            except (ConnectionError, ConnectionResetError):
                dead.append(client_id)

        for client_id in dead:
            del self._clients[client_id]

    async def close(self):

        for client_id, ws in self._clients.items():
            await ws.close()

        self._clients.clear()

        if self._runner:
            await self._runner.cleanup()
            self._runner = None


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

            
class WsClientTransport:

    def __init__(self, url: str = "ws://localhost:8000/ws"):
        self._url = url
        self._session: aiohttp.ClientSession | None = None
        self._ws: aiohttp.ClientWebSocketResponse | None = None

    async def connect(self):
        self._session = aiohttp.ClientSession()
        self._ws = await self._session.ws_connect(self._url)

    async def close(self):
        if self._ws:
            await self._ws.close()
            self._ws = None
        if self._session:
            await self._session.close()
            self._session = None

    async def recv(self) -> bytes:
        if self._ws is None:
            raise ConnectionError("Not connected")
        msg = await self._ws.receive()
        if msg.type == aiohttp.WSMsgType.BINARY:
            return msg.data
        elif msg.type == aiohttp.WSMsgType.TEXT:
            return msg.data.encode()
        elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSING, aiohttp.WSMsgType.CLOSED):
            raise ConnectionError("WebSocket closed")
        else:
            raise ConnectionError(f"Unexpected message type: {msg.type}")

    async def send(self, data: bytes) -> None:
        if self._ws is None:
            raise ConnectionError("Not connected")
        await self._ws.send_bytes(data)

    async def __aenter__(self) -> Self:
        await self.connect()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object
    ) -> None:
        await self.close()

__all__ = [
    "WsClientTransport",
    "WsServerTransport",
    "MulticastWsServerTransport"
]