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
import aiohttp
from aiohttp import web
from wire_rpc.logger import logger


class WsServerTransport:

    def __init__(self, host: str = "0.0.0.0", port: int = 8000, static_dir: str | None = None):
        self._host = host
        self._port = port
        self._static_dir = static_dir
        self._ws: web.WebSocketResponse | None = None
        self._runner: web.AppRunner | None = None
        self._recv_queue: asyncio.Queue[bytes] = asyncio.Queue()
        self._connected = asyncio.Event()

    async def connect(self):
        app = web.Application()
        app.router.add_get("/ws", self._handle_ws)

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
        await self._ws.send_bytes(data)

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