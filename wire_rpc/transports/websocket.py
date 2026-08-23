from typing import Self
from aiohttp import web
import aiohttp
import asyncio


class WebSocketServerTransport:

    def __init__(
        self,
        host: str,
        port: int
    ):
        self._host = host
        self._port = port

    async def connect(self):
        self._app = web.Application()
        self._app.router.add_get("/ws", self._handle)
        self._recv_queue = asyncio.Queue()
        self._send_queue = asyncio.Queue()
        self._runner = web.AppRunner(self._app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, self._host, self._port)
        await site.start()

    async def _handle(self, request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        self._ws = ws
        async for msg in ws:
            if msg.type == aiohttp.WSMsgType.BINARY:
                self._recv_queue.put_nowait(msg.data)
        return ws

    async def recv(self) -> bytes:
        return await self._recv_queue.get()

    async def send(self, data: bytes):
        await self._ws.send_bytes(data)

    async def close(self):
        await self._ws.close()
        await self._runner.cleanup()

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


class WebSocketClientTransport:

    def __init__(
        self,
        url: str
    ):
        self._url = url

    async def connect(self):
        self._session = aiohttp.ClientSession()
        self._ws = await self._session.ws_connect(self._url)

    async def recv(self) -> bytes:
        msg = await self._ws.receive()
        return msg.data

    async def send(self, data: bytes):
        await self._ws.send_bytes(data)

    async def close(self):
        await self._ws.close()
        await self._session.close()

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
    "WebSocketServerTransport",
    "WebSocketClientTransport"
]