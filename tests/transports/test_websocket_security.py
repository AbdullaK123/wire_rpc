import asyncio
from unittest.mock import Mock
import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
from wire_rpc.transports.websocket import MulticastWsServerTransport, WsClientTransport


async def test_unapproved_origin_is_rejected_before_authentication_or_upgrade():
    calls = []
    class Auth:
        async def verify(self, request):
            calls.append('auth'); return 'alice'
    transport = MulticastWsServerTransport(auth=Auth(),allowed_origins={'https://trusted.test'})
    request = Mock(); request.headers={'Origin':'https://evil.test'}
    with pytest.raises(web.HTTPForbidden):
        await transport._handle_ws(request)
    assert calls == [], 'cross-origin requests must not consume authentication resources or inherit cookie authorization'


async def test_revoked_session_is_rechecked_before_an_existing_socket_can_dispatch():
    class Auth:
        valid = True
        async def verify(self, request):
            return 'alice' if self.valid else None
    auth=Auth(); transport=MulticastWsServerTransport(auth=auth)
    app=web.Application(); app.router.add_get('/ws',transport._handle_ws)
    async with TestServer(app) as server, aiohttp.ClientSession() as client:
        ws=await client.ws_connect(server.make_url('/ws'))
        await ws.send_str('request')
        key,_=await transport.recv()
        assert await transport.get_principal(key) == 'alice', 'initial authentication must bind the principal to this physical socket'
        auth.valid=False
        # Concurrently receive the close frame so the real close handshake finishes.
        received=asyncio.create_task(ws.receive())
        with pytest.raises(PermissionError):
            await transport.get_principal(key)
        message=await received
        assert message.type in (aiohttp.WSMsgType.CLOSE,aiohttp.WSMsgType.CLOSED), 'session revocation must close the existing socket rather than merely forgetting its principal'
        await ws.close()


async def test_failed_websocket_upgrade_releases_the_client_session():
    app=web.Application()
    async def reject(request):
        raise web.HTTPForbidden()
    app.router.add_get('/ws',reject)
    async with TestServer(app) as server:
        client=WsClientTransport(str(server.make_url('/ws')))
        with pytest.raises(aiohttp.WSServerHandshakeError):
            await client.connect()
        assert client._session is None and client._ws is None, 'failed upgrades must not leak connectors and sessions across repeated retries'
        await client.close()
