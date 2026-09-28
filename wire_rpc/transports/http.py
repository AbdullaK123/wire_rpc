from collections import deque
import math
from typing import Self
from aiohttp import web
import aiohttp
import asyncio

from wire_rpc.auth.protocol import Authenticator
from wire_rpc.transports.protocol import StartupComponent

class HttpServerTransport:
    """HTTP adapter for the sequential recv -> send transport contract.

    A response belongs to the request returned by recv(), even when that
    request disconnects or times out before send(). Concurrent dispatch needs
    a separate correlation-aware transport API; it cannot use this byte API.
    """

    def __init__(
        self,
        host: str,
        port: int,
        auth: Authenticator | None = None,
        *,
        max_pending_requests: int = 1024,
        request_timeout: float = 30.0,
        max_body_size: int = 4 * 1024 * 1024,
    ):
        if max_pending_requests <= 0 or max_body_size <= 0:
            raise ValueError("HTTP capacity and body limits must be positive")
        if not math.isfinite(request_timeout) or request_timeout <= 0:
            raise ValueError("Request timeout must be finite and positive")
        self._host = host
        self._port = port
        self._auth = auth
        self._max_pending_requests = max_pending_requests
        self._request_timeout = request_timeout
        self._max_body_size = max_body_size
        self._queue: deque[tuple[bytes, asyncio.Future[bytes]]] = deque()
        self._ready = asyncio.Event()
        self._pending: set[asyncio.Future[bytes]] = set()
        self._active: asyncio.Future[bytes] | None = None
        self._receiving = False
        self._closing = False
        self._runner: web.AppRunner | None = None

    async def startup(self):
        if self._auth and isinstance(self._auth, StartupComponent):
            await self._auth.startup()

    async def shutdown(self):
        if self._auth and isinstance(self._auth, StartupComponent):
            await self._auth.shutdown()

    async def connect(self):
        self._app = web.Application(client_max_size=self._max_body_size)
        self._app.router.add_post("/rpc", self._handle)
        if self._auth:
            self._app.router.add_post("/login", self._auth.login)
            self._app.router.add_post("/logout", self._auth.logout)
        self._runner = web.AppRunner(self._app, handler_cancellation=True)
        await self._runner.setup()
        site = web.TCPSite(self._runner, self._host, self._port)
        await site.start()

    async def _handle(self, request: web.Request) -> web.Response:
        if self._closing or len(self._pending) >= self._max_pending_requests:
            raise web.HTTPServiceUnavailable(text="RPC capacity unavailable")

        # Reserve before authentication/body reads so admission cannot race.
        response = asyncio.get_running_loop().create_future()
        self._pending.add(response)
        entry = None
        try:
            async with asyncio.timeout(self._request_timeout):
                if self._auth:
                    user_id = await self._auth.verify(request)
                    if user_id is None:
                        raise web.HTTPUnauthorized(text="Invalid credentials")
                data = await request.read()
                if self._closing:
                    raise web.HTTPServiceUnavailable(text="RPC transport closed")
                entry = (data, response)
                self._queue.append(entry)
                self._ready.set()
                response_data = await response
                return web.Response(body=response_data)
        except TimeoutError as exc:
            raise web.HTTPGatewayTimeout(text="RPC request timed out") from exc
        finally:
            self._pending.discard(response)
            if not response.done():
                response.cancel()
            if entry is not None:
                try:
                    self._queue.remove(entry)
                except ValueError:
                    pass  # recv() already owns it; retain _active until send().

    async def recv(self) -> bytes:
        if self._receiving or self._active is not None:
            raise RuntimeError("Send the current response before receiving again")
        self._receiving = True
        try:
            while True:
                if self._closing:
                    raise ConnectionError("HTTP transport closed")
                while self._queue:
                    data, response = self._queue.popleft()
                    if not response.done():
                        self._active = response
                        return data
                self._ready.clear()
                await self._ready.wait()
        finally:
            self._receiving = False

    async def send(self, data: bytes):
        if self._closing:
            raise ConnectionError("HTTP transport closed")
        response = self._active
        if response is None:
            raise RuntimeError("No HTTP request owns this response")
        self._active = None
        if not response.done():
            response.set_result(data)

    async def close(self):
        self._closing = True
        self._ready.set()
        for response in self._pending:
            if not response.done():
                response.set_exception(web.HTTPServiceUnavailable(text="RPC transport closed"))
                # Also mark observed when a handler is still reading its body.
                response.exception()
        self._queue.clear()
        self._active = None
        if self._runner is not None:
            await self._runner.cleanup()
            self._runner = None

    async def __aenter__(self) -> Self:
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
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
