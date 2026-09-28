from wire_rpc.auth.protocol import InteractiveAuthenticator
from wire_rpc.auth.errors import AuthUnavailableError
from collections import deque
import math
import ssl
from wire_rpc._validation import positive_timeout, positive_limit
from typing import Self
from collections.abc import Mapping
from aiohttp import web
import aiohttp
from aiohttp.abc import AbstractCookieJar
import asyncio

from wire_rpc.auth.protocol import Authenticator
from wire_rpc.transports.protocol import StartupComponent
from wire_rpc.transports.tcp._connection_limiter import ConnectionLimiter, ConnectionLimitExceeded

class HttpServerTransport:
    """HTTP adapter for the sequential recv -> send transport contract.

    A response belongs to the request returned by recv(), even when that
    request disconnects or times out before send(). Concurrent dispatch needs
    a separate correlation-aware transport API; it cannot use this byte API.
    """

    requires_text_codec = True

    def __init__(
        self,
        host: str,
        port: int,
        auth: Authenticator | None = None,
        *,
        max_pending_requests: int = 64,
        request_timeout: float = 30.0,
        max_body_size: int = 1024 * 1024,
        max_queue_bytes: int = 16 * 1024 * 1024,
        shutdown_timeout: float = 10.0,
        ssl_context: ssl.SSLContext | None = None,
        allowed_origins: set[str] | None = None,
    ):
        positive_limit("max_pending_requests", max_pending_requests)
        positive_limit("max_body_size", max_body_size)
        if max_pending_requests <= 0 or max_body_size <= 0:
            raise ValueError("HTTP capacity and body limits must be positive")
        positive_timeout("request_timeout", request_timeout)
        if not math.isfinite(request_timeout) or request_timeout <= 0:
            raise ValueError("Request timeout must be finite and positive")
        positive_limit('max_queue_bytes', max_queue_bytes)
        positive_timeout('shutdown_timeout', shutdown_timeout)
        self._max_queue_bytes = max_queue_bytes
        self._queued_bytes = 0
        self._principals: dict[asyncio.Future[bytes | None], tuple[str | None, web.Request]] = {}
        self._active_principal: str | None = None
        self._active_request: web.Request | None = None
        self._shutdown_timeout = shutdown_timeout
        self._ssl = ssl_context
        self._allowed_origins = frozenset(allowed_origins or ())
        self._host = host
        self._port = port
        self._auth = auth
        self._auth_slots = ConnectionLimiter(16)
        self._max_pending_requests = max_pending_requests
        self._request_timeout = request_timeout
        self._max_body_size = max_body_size
        self._queue: deque[tuple[bytes, asyncio.Future[bytes | None]]] = deque()
        self._ready = asyncio.Event()
        self._pending: set[asyncio.Future[bytes | None]] = set()
        self._active: asyncio.Future[bytes | None] | None = None
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
        if self._runner is not None or self._closing:
            raise RuntimeError("Transport already listening or closed")
        self._app = web.Application(client_max_size=self._max_body_size)
        self._app.router.add_post("/rpc", self._handle)
        auth = self._auth
        if isinstance(auth, InteractiveAuthenticator):
            async def login(request):
                return await self._auth_request(request, auth.login)
            async def logout(request):
                return await self._auth_request(request, auth.logout)
            self._app.router.add_post('/login', login)
            self._app.router.add_post('/logout', logout)
        self._runner = web.AppRunner(self._app, handler_cancellation=True, shutdown_timeout=self._shutdown_timeout)
        try:
            await self._runner.setup()
            site = web.TCPSite(self._runner, self._host, self._port, ssl_context=self._ssl)
            await site.start()
        except BaseException:
            await self.close()
            raise

    async def _auth_request(self, request, handler):
        origin = request.headers.get('Origin')
        if isinstance(origin,str) and origin not in self._allowed_origins:
            raise web.HTTPForbidden(text='Origin not allowed')
        try:
            async with self._auth_slots.slot():
                async with asyncio.timeout(self._request_timeout):
                    return await handler(request)
        except ConnectionLimitExceeded:
            raise web.HTTPServiceUnavailable(text='Authentication capacity unavailable') from None
        except TimeoutError:
            raise web.HTTPGatewayTimeout(text='Authentication timed out') from None

    async def _handle(self, request: web.Request) -> web.Response:
        if self._closing or len(self._pending) >= self._max_pending_requests:
            raise web.HTTPServiceUnavailable(text="RPC capacity unavailable")

        # Reserve before authentication/body reads so admission cannot race.
        origin = request.headers.get('Origin')
        if isinstance(origin, str) and origin not in self._allowed_origins:
            raise web.HTTPForbidden(text='Origin not allowed')
        response: asyncio.Future[bytes | None] = asyncio.get_running_loop().create_future()
        self._pending.add(response)
        entry = None
        try:
            async with asyncio.timeout(self._request_timeout):
                user_id = None
                if self._auth:
                    user_id = await self._auth.verify(request)
                    if user_id is None:
                        raise web.HTTPUnauthorized(text="Invalid credentials")
                data = await request.read()
                if self._closing:
                    raise web.HTTPServiceUnavailable(text="RPC transport closed")
                if len(data) > self._max_body_size:
                    raise web.HTTPRequestEntityTooLarge(max_size=self._max_body_size, actual_size=len(data))
                if self._queued_bytes + len(data) > self._max_queue_bytes:
                    raise web.HTTPServiceUnavailable(text='RPC byte capacity unavailable')
                self._principals[response] = (user_id, request)
                self._queued_bytes += len(data)
                entry = (data, response)
                self._queue.append(entry)
                self._ready.set()
                response_data = await response
                return web.Response(status=204) if response_data is None else web.Response(body=response_data, content_type="application/json")
        except AuthUnavailableError:
            raise web.HTTPServiceUnavailable(text="Authentication unavailable") from None
        except TimeoutError as exc:
            raise web.HTTPGatewayTimeout(text="RPC request timed out") from exc
        finally:
            self._pending.discard(response)
            self._principals.pop(response, None)
            if not response.done():
                response.cancel()
            if entry is not None:
                try:
                    self._queue.remove(entry)
                    self._queued_bytes -= len(entry[0])
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
                    self._queued_bytes -= len(data)
                    if not response.done():
                        self._active = response
                        self._active_principal, self._active_request = self._principals[response]
                        return data
                self._ready.clear()
                await self._ready.wait()
        finally:
            self._receiving = False

    @property
    def max_message_size(self):
        return self._max_body_size

    @property
    def stats(self):
        return {'pending': len(self._pending), 'queued_bytes': self._queued_bytes}

    async def get_principal(self, client_id=None):
        if self._active is None or self._active.done():
            raise PermissionError('Request no longer active')
        if self._auth:
            principal = await self._auth.verify(self._active_request)
            if principal is None or principal != self._active_principal:
                raise PermissionError('Session no longer valid')
        if self._active is None or self._active.done() or self._closing:
            raise PermissionError('Request no longer active')
        return self._active_principal

    async def finish_notification(self):
        response, self._active = self._active, None
        self._active_principal = self._active_request = None
        if response is not None and not response.done():
            response.set_result(None)

    async def send(self, data: bytes):
        if len(data) > self._max_body_size:
            raise ValueError('Response exceeds HTTP payload limit')
        if self._closing:
            raise ConnectionError("HTTP transport closed")
        response = self._active
        if response is None:
            raise RuntimeError("No HTTP request owns this response")
        self._active = None
        self._active_principal = self._active_request = None
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
        self._queued_bytes = 0
        self._active = None
        self._active_principal = self._active_request = None
        if self._runner is not None:
            await self._runner.cleanup()
            self._runner = None

    async def __aenter__(self) -> Self:
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()


class HttpClientTransport:
    requires_text_codec = True

    def __init__(self, url: str, *, request_timeout=30.0, max_body_size=1024 * 1024,
                 headers: Mapping[str, str] | None = None, cookie_jar: AbstractCookieJar | None = None,
                 ssl_context: ssl.SSLContext | None = None, allow_insecure_credentials: bool = False):
        positive_timeout('request_timeout', request_timeout)
        positive_limit('max_body_size', max_body_size)
        from wire_rpc.auth.client import validate_client_credentials
        validate_client_credentials(url, headers, cookie_jar, ssl_context, allow_insecure_credentials)
        self._headers = dict(headers or {})
        self._cookie_jar = cookie_jar
        self._ssl_context = ssl_context
        self._url = url
        self._timeout = request_timeout
        self._max_body_size = max_body_size
        self._session: aiohttp.ClientSession | None = None
        self._pending: aiohttp.ClientResponse | None = None
        self._sending = False

    async def connect(self):
        if self._session is not None:
            raise RuntimeError('Already connected')
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self._timeout),
            connector=aiohttp.TCPConnector(limit=1, ssl=self._ssl_context if self._ssl_context is not None else True),
            headers=self._headers, cookie_jar=self._cookie_jar,
            auto_decompress=False,
        )

    async def send(self, data: bytes):
        if self._session is None:
            raise ConnectionError('Not connected')
        if self._pending is not None or self._sending:
            raise RuntimeError('Receive the current response before sending again')
        if len(data) > self._max_body_size:
            raise ValueError('Request exceeds HTTP payload limit')
        self._sending = True
        try:
            response = await self._session.post(self._url, data=data,
                headers={'Content-Type':'application/json'}, allow_redirects=False)
            if response.status != 200:
                response.close()
                raise ConnectionError('RPC HTTP request failed')
            self._pending = response
        finally:
            self._sending = False

    async def recv(self) -> bytes:
        response = self._pending
        if response is None:
            raise RuntimeError('No pending HTTP response')
        try:
            if response.content_length is not None and response.content_length > self._max_body_size:
                raise ConnectionError('RPC response exceeds byte limit')
            body = bytearray()
            async for chunk in response.content.iter_chunked(65536):
                if len(body) + len(chunk) > self._max_body_size:
                    raise ConnectionError('RPC response exceeds byte limit')
                body.extend(chunk)
            return bytes(body)
        finally:
            response.close()
            self._pending = None

    async def close(self):
        if self._pending is not None:
            self._pending.close()
            self._pending = None
        if self._session is not None:
            await self._session.close()
            self._session = None

    async def __aenter__(self) -> Self:
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()


__all__ = ['HttpServerTransport', 'HttpClientTransport']
