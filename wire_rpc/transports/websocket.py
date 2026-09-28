from wire_rpc.auth.protocol import InteractiveAuthenticator
from wire_rpc.auth.errors import AuthUnavailableError
"""Bounded browser-oriented WebSocket transports with explicit ownership."""
import asyncio
from pathlib import Path
import ssl
import uuid
from typing import Any, Callable, cast
from collections.abc import Mapping

import aiohttp
from aiohttp.abc import AbstractCookieJar
from aiohttp import web

from wire_rpc._validation import positive_limit, positive_timeout
from wire_rpc.transports._queue import PayloadQueue
from wire_rpc.transports.errors import TransportError
from wire_rpc.transports.protocol import StartupComponent
from wire_rpc.transports.tcp._connection_limiter import ConnectionLimiter, ConnectionLimitExceeded


def _decode_text_payload(data: bytes) -> str:
    try:
        return data.decode('utf-8')
    except UnicodeDecodeError as exc:
        raise TransportError('Payload was not utf-8 encoded.') from exc


class _WsServer:
    requires_text_codec = True
    _multicast = False

    def _configure(self, host, port, max_msg_size, recv_queue_size, write_timeout,
                   max_connections, static_dir, auth, max_queue_bytes, per_peer_queue_size,
                   auth_timeout, close_timeout, allowed_origins, ssl_context):
        for name, value in [('max_msg_size',max_msg_size),('max_connections',max_connections)]:
            positive_limit(name, value)
        for name, value in [('write_timeout',write_timeout),('auth_timeout',auth_timeout),('close_timeout',close_timeout)]:
            positive_timeout(name, value)
        if max_msg_size > max_queue_bytes:
            raise ValueError('A message must fit the queue byte budget')
        self._host, self._port = host, port
        self._max_msg_size = max_msg_size
        self._write_timeout, self._auth_timeout, self._close_timeout = write_timeout, auth_timeout, close_timeout
        self._static_dir, self._auth, self._ssl = static_dir, auth, ssl_context
        self._allowed_origins = frozenset(allowed_origins or ())
        self._connection_limiter = ConnectionLimiter(max_connections)
        self._auth_slots = ConnectionLimiter(16)
        self._recv_queue: PayloadQueue[Any] = PayloadQueue(recv_queue_size, max_bytes=max_queue_bytes, per_peer=per_peer_queue_size)
        self._runner = None
        self._ws = None
        self._clients = {}
        self._requests = {}
        self._principals = {}
        self._connected = asyncio.Event()
        self._closing = False
        self._session_used = False

    @property
    def max_message_size(self):
        return self._max_msg_size

    @property
    def stats(self):
        return {**self._recv_queue.stats, 'connections':len(self._clients)}

    async def startup(self):
        if self._auth and isinstance(self._auth, StartupComponent):
            await self._auth.startup()

    async def shutdown(self):
        if self._auth and isinstance(self._auth, StartupComponent):
            await self._auth.shutdown()

    def _check_origin(self, request):
        origin = request.headers.get('Origin')
        if isinstance(origin, str) and origin not in self._allowed_origins:
            raise web.HTTPForbidden(text='Origin not allowed')

    async def _auth_request(self, request, handler):
        self._check_origin(request)
        try:
            async with self._auth_slots.slot():
                async with asyncio.timeout(self._auth_timeout):
                    return await handler(request)
        except ConnectionLimitExceeded:
            raise web.HTTPServiceUnavailable(text='Authentication capacity unavailable') from None
        except TimeoutError:
            raise web.HTTPGatewayTimeout(text='Authentication timed out') from None

    async def _start(self):
        if self._closing or self._runner is not None:
            raise RuntimeError('Transport is closed or already listening')
        app = web.Application(client_max_size=self._max_msg_size)
        app.router.add_get('/ws', self._handle_ws)
        if isinstance(self._auth, InteractiveAuthenticator):
            async def login(request):
                return await self._auth_request(request, self._auth.login)
            async def logout(request):
                return await self._auth_request(request, self._auth.logout)
            app.router.add_post('/login', login)
            app.router.add_post('/logout', logout)
        if self._static_dir:
            static_path = Path(self._static_dir)
            async def index(request):
                return web.FileResponse(static_path / 'index.html')
            app.router.add_get('/', index)
            app.router.add_static('/static', static_path)
        self._runner = web.AppRunner(app, shutdown_timeout=self._close_timeout)
        try:
            await self._runner.setup()
            await web.TCPSite(self._runner, self._host, self._port, ssl_context=self._ssl).start()
        except BaseException:
            await self.close()
            raise

    async def connect(self):
        await self._start()
        if not self._multicast:
            await self._connected.wait()
            if self._closing:
                raise ConnectionError('Transport closed')

    async def _handle_ws(self, request):
        if self._closing or (not self._multicast and self._session_used):
            raise web.HTTPServiceUnavailable(text='Connection unavailable')
        self._check_origin(request)
        try:
            async with self._connection_limiter.slot():
                return await self._serve_ws(request)
        except ConnectionLimitExceeded as exc:
            raise web.HTTPServiceUnavailable(text='No connection slot available') from exc

    async def _serve_ws(self, request):
        principal = None
        if self._auth:
            try:
                async with asyncio.timeout(self._auth_timeout):
                    principal = await self._auth.verify(request)
            except AuthUnavailableError:
                raise web.HTTPServiceUnavailable(text='Authentication unavailable') from None
            except TimeoutError:
                raise web.HTTPGatewayTimeout(text='Authentication timed out') from None
            if principal is None:
                raise web.HTTPUnauthorized(text='Invalid credentials')
        ws = web.WebSocketResponse(max_msg_size=self._max_msg_size, heartbeat=30.0,
                                   timeout=self._close_timeout, autoping=True, autoclose=True,
                                   compress=False)
        async with asyncio.timeout(self._auth_timeout):
            await ws.prepare(request)
        if self._closing:
            await self._close_ws(ws)
            return ws
        client_id = str(uuid.uuid4()) if self._multicast else 'unicast'
        self._clients[client_id] = ws
        self._requests[client_id] = request
        self._principals[client_id] = principal
        if not self._multicast:
            self._ws = ws
            self._session_used = True
        self._connected.set()
        try:
            async for msg in ws:
                if msg.type in (aiohttp.WSMsgType.BINARY, aiohttp.WSMsgType.TEXT):
                    data = msg.data.encode('utf-8') if msg.type == aiohttp.WSMsgType.TEXT else msg.data
                    # Encode can expand decoded text; enforce the actual byte budget.
                    if len(data) > self._max_msg_size:
                        break
                    await self._recv_queue.put((client_id,data) if self._multicast else data)
        except ConnectionError:
            pass
        finally:
            if self._clients.get(client_id) is ws:
                self._clients.pop(client_id, None)
                self._requests.pop(client_id, None)
                self._principals.pop(client_id, None)
            self._recv_queue.drop_peer(client_id if self._multicast else None)
            if not self._multicast:
                self._ws = None
                self._recv_queue.close()
            await self._close_ws(ws)
        return ws

    async def get_principal(self, client_id=None):
        key = client_id if self._multicast else 'unicast'
        ws = self._clients.get(key)
        if ws is None:
            raise PermissionError('Peer disconnected')
        principal = self._principals[key]
        if self._auth:
            async with asyncio.timeout(self._auth_timeout):
                current = await self._auth.verify(self._requests[key])
            if current is None or current != principal:
                await self._close_ws(ws)
                raise PermissionError('Session expired or revoked')
        if self._closing or self._clients.get(key) is not ws:
            raise PermissionError('Peer disconnected during validation')
        return principal

    async def _close_ws(self, ws):
        try:
            async with asyncio.timeout(self._close_timeout):
                await ws.close()
        except (TimeoutError, ConnectionError):
            pass

    async def recv(self):
        return await self._recv_queue.get()

    async def _send(self, ws, data):
        if len(data) > self._max_msg_size:
            raise TransportError('WebSocket payload exceeds byte limit')
        if ws is None:
            raise ConnectionError('Peer not connected')
        try:
            async with asyncio.timeout(self._write_timeout):
                await ws.send_str(_decode_text_payload(data))
        except (TimeoutError, ConnectionError, asyncio.CancelledError):
            await self._close_ws(ws)
            raise

    async def close(self):
        self._closing = True
        self._connected.set()
        self._recv_queue.close()
        await asyncio.gather(*(self._close_ws(ws) for ws in list(self._clients.values())))
        self._clients.clear()
        self._requests.clear()
        self._principals.clear()
        self._ws = None
        if self._runner is not None:
            runner, self._runner = self._runner, None
            async with asyncio.timeout(self._close_timeout * 2):
                await runner.cleanup()

    async def __aenter__(self):
        # Bind failures surface before the application enters its receive loop.
        await self._start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()


class WsServerTransport(_WsServer):
    def __init__(self, host='0.0.0.0', port=8000, max_msg_size=1024*1024,
                 recv_queue_size=256, write_timeout=10.0, static_dir=None, auth=None,
                 *, max_queue_bytes=16*1024*1024, auth_timeout=10.0, close_timeout=5.0,
                 allowed_origins=None, ssl_context: ssl.SSLContext | None=None):
        self._configure(host,port,max_msg_size,recv_queue_size,write_timeout,1,static_dir,auth,
                        max_queue_bytes,recv_queue_size,auth_timeout,close_timeout,allowed_origins,ssl_context)

    async def send(self, data):
        await self._send(self._ws, data)


class MulticastWsServerTransport(_WsServer):
    _multicast = True

    def __init__(self, host='0.0.0.0', port=8000, max_msg_size=1024*1024,
                 recv_queue_size=256, write_timeout=10.0, max_connections=64,
                 static_dir=None, auth=None, *, max_queue_bytes=16*1024*1024,
                 per_peer_queue_size=16, auth_timeout=10.0, close_timeout=5.0,
                 allowed_origins=None, ssl_context: ssl.SSLContext | None=None):
        self._configure(host,port,max_msg_size,recv_queue_size,write_timeout,max_connections,static_dir,auth,
                        max_queue_bytes,per_peer_queue_size,auth_timeout,close_timeout,allowed_origins,ssl_context)

    async def send(self, client_id, data):
        await self._send(self._clients.get(client_id), data)

    async def broadcast(self, data):
        async def deliver(client_id, ws):
            try:
                await self._send(ws, data)
            except (ConnectionError, TimeoutError):
                if self._clients.get(client_id) is ws:
                    self._clients.pop(client_id, None)
                await self._close_ws(ws)
        # Bounded by admission. A slow peer does not serialize every other send.
        await asyncio.gather(*(deliver(key, ws) for key,ws in list(self._clients.items())))


class WsClientTransport:
    requires_text_codec = True

    def __init__(self, url='ws://localhost:8000/ws', receive_timeout=10.0, close_timeout=5.0,
                 *, connect_timeout=10.0, write_timeout=10.0, max_msg_size=1024*1024,
                 headers: Mapping[str, str] | None = None, cookie_jar: AbstractCookieJar | None = None,
                 ssl_context: ssl.SSLContext | None = None, allow_insecure_credentials: bool = False):
        for name,value in [('receive_timeout',receive_timeout),('close_timeout',close_timeout),
                           ('connect_timeout',connect_timeout),('write_timeout',write_timeout)]:
            positive_timeout(name,value)
        positive_limit('max_msg_size',max_msg_size)
        from wire_rpc.auth.client import validate_client_credentials
        validate_client_credentials(url, headers, cookie_jar, ssl_context, allow_insecure_credentials)
        self._headers = dict(headers or {})
        self._cookie_jar, self._ssl_context = cookie_jar, ssl_context
        self._url = url
        self._receive_timeout, self._close_timeout = receive_timeout, close_timeout
        self._connect_timeout, self._write_timeout = connect_timeout, write_timeout
        self._max_msg_size = max_msg_size
        self._session: aiohttp.ClientSession | None = None
        self._ws: aiohttp.ClientWebSocketResponse | None = None

    async def connect(self):
        if self._session is not None:
            raise RuntimeError('Already connected')
        trace = aiohttp.TraceConfig()
        async def reject_redirect(session, context, params):
            params.response.close()
            raise ConnectionError('WebSocket redirects are disabled to protect credentials')
        trace.on_request_redirect.append(reject_redirect)
        self._session = aiohttp.ClientSession(headers=self._headers, cookie_jar=self._cookie_jar, trace_configs=[trace])
        try:
            async with asyncio.timeout(self._connect_timeout):
                self._ws = await self._session.ws_connect(self._url, max_msg_size=self._max_msg_size,
                    ssl=self._ssl_context if self._ssl_context is not None else True,
                    # aiohttp uses legacy attrs fields, whose generated constructor
                    # Pyright cannot infer. Keep the actual runtime timeout type.
                    timeout=cast(Callable[..., aiohttp.ClientWSTimeout], aiohttp.ClientWSTimeout)(
                        ws_close=self._close_timeout), compress=0)
        except BaseException:
            await self.close()
            raise

    async def close(self):
        try:
            if self._ws is not None:
                async with asyncio.timeout(self._close_timeout):
                    await self._ws.close()
        finally:
            self._ws = None
            if self._session is not None:
                await self._session.close()
                self._session = None

    async def recv(self):
        if self._ws is None:
            raise ConnectionError('Not connected')
        async with asyncio.timeout(self._receive_timeout):
            msg = await self._ws.receive()
        if msg.type == aiohttp.WSMsgType.BINARY:
            return msg.data
        if msg.type == aiohttp.WSMsgType.TEXT:
            return msg.data.encode('utf-8')
        raise ConnectionError('WebSocket closed or invalid message')

    async def send(self, data):
        if self._ws is None:
            raise ConnectionError('Not connected')
        if len(data) > self._max_msg_size:
            raise TransportError('WebSocket payload exceeds byte limit')
        async with asyncio.timeout(self._write_timeout):
            await self._ws.send_str(_decode_text_payload(data))

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()


__all__ = ['WsClientTransport','WsServerTransport','MulticastWsServerTransport']
