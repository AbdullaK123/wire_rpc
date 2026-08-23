from typing import Self
from aiohttp import web
import aiohttp
import asyncio

from wire_rpc.auth.protocol import Authenticator

class HttpServerTransport:

    def __init__(
        self,
        host: str,
        port: int,
        auth: Authenticator | None = None
    ):
        self._host = host
        self._port = port
        self._auth = auth

    async def connect(self):
        self._app = web.Application()
        self._app.router.add_post("/rpc", self._handle)

        if self._auth:
            self._app.router.add_post("/login", self._auth.login)

        self._queue = asyncio.Queue()
        self._response_queue = asyncio.Queue()
        self._runner = web.AppRunner(self._app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, self._host, self._port)
        await site.start()

    async def _handle(self, request: web.Request) -> web.Response:

        if self._auth:
            user_id = self._auth.verify(request)
            if user_id is None:
                raise web.HTTPUnauthorized(text="Invalid credentials")

        data = await request.read()
        self._queue.put_nowait(data)
        response_data = await self._response_queue.get()
        return web.Response(body=response_data)

    async def recv(self) -> bytes:
        return await self._queue.get()

    async def send(self, data: bytes):
        self._response_queue.put_nowait(data)

    async def close(self):
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


class HttpClientTransport:

    def __init__(
        self,
        url: str
    ):
        self._url = url

    async def connect(self):
         self._session = aiohttp.ClientSession()

    async def recv(self) -> bytes:
         data = await self._pending.read()
         return data

    async def send(self, data: bytes):
         self._pending = await self._session.post(
             self._url,
             data=data
         )

    async def close(self):
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
    "HttpServerTransport",
    "HttpClientTransport"
]
