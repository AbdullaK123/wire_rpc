import pytest
import aiohttp
from aiohttp import web
from wire_rpc.transports.http import HttpClientTransport
from wire_rpc.transports.websocket import WsClientTransport
from tests.helpers import listening_port


@pytest.mark.parametrize('factory,url', [(HttpClientTransport,'http://localhost/rpc'), (WsClientTransport,'ws://localhost/ws')])
async def test_plaintext_client_configuration_cannot_transmit_configured_credentials(factory, url):
    with pytest.raises(ValueError):
        factory(url, headers={'Authorization':'Bearer secret'})
    with pytest.raises(ValueError):
        factory(url, cookie_jar=aiohttp.CookieJar())


async def test_websocket_redirect_cannot_exfiltrate_custom_api_key_headers():
    stolen = []
    async def sink(request):
        stolen.append(request.headers.get('X-Custom-Secret'))
        return web.Response()
    target = web.Application(); target.router.add_get('/steal', sink)
    target_runner = web.AppRunner(target); await target_runner.setup()
    await web.TCPSite(target_runner,'127.0.0.1',0).start()
    async def redirect(request):
        raise web.HTTPFound(f'http://127.0.0.1:{listening_port(target_runner)}/steal')
    source = web.Application(); source.router.add_get('/ws',redirect)
    source_runner = web.AppRunner(source); await source_runner.setup()
    await web.TCPSite(source_runner,'127.0.0.1',0).start()
    client = WsClientTransport(f'ws://127.0.0.1:{listening_port(source_runner)}/ws',
        headers={'X-Custom-Secret':'VERY_SECRET'}, allow_insecure_credentials=True)
    try:
        with pytest.raises(ConnectionError):
            await client.connect()
        assert stolen == [], 'redirect handling must never forward custom credential headers to an attacker-controlled destination'
        assert client._session is None, 'a rejected authentication redirect must not leak its client session'
    finally:
        await client.close(); await source_runner.cleanup(); await target_runner.cleanup()
