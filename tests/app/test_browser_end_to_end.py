from tests.helpers import listening_port
import asyncio
import aiohttp
import pytest
from wire_rpc import App, MulticastApp, Client, current_request
from wire_rpc.codecs.msgspec import MsgSpecJsonCodec
from wire_rpc.transports.http import HttpServerTransport,HttpClientTransport
from wire_rpc.transports.websocket import MulticastWsServerTransport,WsClientTransport


@pytest.mark.parametrize('kind',['http','ws'])
async def test_authenticated_browser_transport_preserves_identity_after_rejected_params(kind):
    ready=asyncio.Event()
    class Auth:
        async def verify(self,request):
            return 'alice'
        async def login(self,request):
            pass
        async def logout(self,request):
            pass
    transport: HttpServerTransport | MulticastWsServerTransport
    app: App | MulticastApp
    if kind == 'http':
        class HttpServer(HttpServerTransport):
            async def connect(self):
                await super().connect(); ready.set()
        transport=HttpServer('127.0.0.1',0,auth=Auth())
        app=App(transport)
    else:
        class WsServer(MulticastWsServerTransport):
            async def _start(self):
                await super()._start(); ready.set()
        transport=WsServer('127.0.0.1',0,auth=Auth())
        app=MulticastApp(transport)
    calls=[]
    @app.method('write')
    async def write(params: dict,ctx):
        calls.append(params)
        return current_request().principal
    task=asyncio.create_task(app._run()); await ready.wait()
    port=listening_port(transport._runner)
    wire=HttpClientTransport(f'http://127.0.0.1:{port}/rpc') if kind=='http' else WsClientTransport(f'ws://127.0.0.1:{port}/ws')
    client=Client(wire)
    try:
        await client.connect()
        from wire_rpc.client import WireRpcError
        with pytest.raises(WireRpcError) as error:
            await client.call('write',str,params=None)
        assert error.value.code == -32602 and not calls, 'invalid browser inputs must be rejected before the authenticated handler performs any mutation'
        assert await client.call('write',str,params={}) == 'alice', 'a rejected request must not erase or swap the principal of the next request'
    finally:
        await client.close(); await app.stop(); await task
    assert transport._runner is None, 'application teardown must release the actual HTTP listener'


async def test_http_notification_releases_its_response_slot_with_no_rpc_body():
    ready=asyncio.Event()
    class HttpServer(HttpServerTransport):
        async def connect(self):
            await super().connect(); ready.set()
    transport=HttpServer('127.0.0.1',0); app=App(transport)
    @app.method('notify')
    async def notify(ctx):
        return 'must not be returned'
    task=asyncio.create_task(app._run()); await ready.wait()
    port=listening_port(transport._runner)
    try:
        async with aiohttp.ClientSession() as client:
            async with client.post(f'http://127.0.0.1:{port}/rpc',json={'jsonrpc':'2.0','method':'notify'}) as response:
                assert response.status == 204 and await response.read() == b'', 'notifications must complete their HTTP exchange without emitting an unowned JSON-RPC reply'
        assert transport._active is None, 'a notification must release HTTP response ownership or the next recv will fail'
    finally:
        await app.stop(); await task
