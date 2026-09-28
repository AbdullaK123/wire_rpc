import asyncio
from unittest.mock import AsyncMock, Mock
import aiohttp
import pytest
from yarl import URL
from wire_rpc import App, MulticastApp, Client, current_request
from wire_rpc.auth import BearerAuth, ApiKeyAuth, CookieSessionAuth, credential_headers
from wire_rpc.auth.credentials import SessionTokenValidator
from wire_rpc.auth.sessions import InMemorySessionStore
from wire_rpc.transports.http import HttpServerTransport, HttpClientTransport
from wire_rpc.transports.websocket import MulticastWsServerTransport, WsClientTransport
from wire_rpc.client import WireRpcError
from tests.helpers import listening_port


@pytest.mark.parametrize('kind', ['http','ws'])
@pytest.mark.parametrize('scheme', ['bearer','api-key','cookie'])
async def test_revoking_credentials_blocks_mutations_on_an_existing_browser_transport(kind, scheme):
    store = InMemorySessionStore()
    token = await store.create('alice')
    validator = SessionTokenValidator(store)
    auth = (BearerAuth(validator, require_tls=False) if scheme=='bearer' else
            ApiKeyAuth(validator, require_tls=False) if scheme=='api-key' else
            CookieSessionAuth(Mock(validate=AsyncMock()), store, secure=False))
    ready = asyncio.Event()
    class HttpServer(HttpServerTransport):
        async def connect(self):
            await super().connect(); ready.set()
    class WsServer(MulticastWsServerTransport):
        async def _start(self):
            await super()._start(); ready.set()
    transport: HttpServerTransport | MulticastWsServerTransport
    app: App | MulticastApp
    if kind == 'http':
        transport = HttpServer('127.0.0.1',0,auth=auth)
        app = App(transport)
    else:
        transport = WsServer('127.0.0.1',0,auth=auth)
        app = MulticastApp(transport)
    calls = []
    @app.method('mutate')
    async def mutate(ctx):
        calls.append(current_request().principal)
        return current_request().principal
    running = asyncio.create_task(app._run()); await ready.wait()
    port = listening_port(transport._runner)
    jar = aiohttp.CookieJar(unsafe=True)
    headers = {}
    if scheme == 'cookie':
        jar.update_cookies({'session':token}, response_url=URL(f'http://127.0.0.1:{port}'))
    else:
        headers = dict(credential_headers(bearer=token) if scheme=='bearer' else credential_headers(api_key=token))
    wire = (HttpClientTransport(f'http://127.0.0.1:{port}/rpc',headers=headers,cookie_jar=jar,allow_insecure_credentials=True) if kind=='http'
            else WsClientTransport(f'ws://127.0.0.1:{port}/ws',headers=headers,cookie_jar=jar,allow_insecure_credentials=True))
    client = Client(wire)
    try:
        await client.connect()
        assert await client.call('mutate',str) == 'alice', 'the established channel must carry the configured credentials into request context'
        await store.revoke_all('alice')
        with pytest.raises((ConnectionError, WireRpcError, aiohttp.ClientError)):
            await client.call('mutate',str)
        assert calls == ['alice'], 'revocation must be checked before another mutation even when the connection was previously authenticated'
    finally:
        await client.close(); await app.stop(); await running
