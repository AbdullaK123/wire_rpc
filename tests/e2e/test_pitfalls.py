"""Regression tests for two former pitfalls: the WebSocket client's 10-second reply
deadline, and browser requests being rejected unless allowed_origins was set."""
import asyncio
import logging

import aiohttp
import pytest
from argon2 import PasswordHasher

from tests.e2e.harness import serve
from wire_rpc import Client, MulticastApp
from wire_rpc.auth import CookieSessionAuth
from wire_rpc.auth.credentials import PasswordAccount, StaticPasswordCredentialValidator
from wire_rpc.transports.websocket import WsClientTransport

pytestmark = pytest.mark.timeout(30)


@pytest.mark.timeout(45)
async def test_websocket_client_gives_up_on_slow_methods_and_idle_listeners():
    gate = asyncio.Event()

    def setup(app):
        @app.method('slow')
        async def slow(ctx) -> str:
            await asyncio.sleep(11)          # longer than the old 10-second receive_timeout
            return 'done'

    async with serve('ws-multi', setup=setup) as server:
        # Built directly, not with server.wire(), so the transport keeps its library defaults.
        url = f'ws://127.0.0.1:{server.port}/ws'
        caller = Client(WsClientTransport(url), call_timeout=20)
        listener = Client(WsClientTransport(url), call_timeout=20)
        async with caller, listener:
            assert await listener.call('add', int, {'a': 0, 'b': 0}) == 0   # registered for broadcasts
            slow_call = asyncio.create_task(caller.call('slow', str))
            waiting = asyncio.create_task(listener.receive_notification())
            await asyncio.sleep(11)          # the listener sits idle past the old deadline too
            assert isinstance(server.app, MulticastApp)
            await server.app.broadcast('tick', {'n': 1})
            gate.set()
            assert await slow_call == 'done', 'a method slower than 10 s must succeed when call_timeout allows it'
            note = await asyncio.wait_for(waiting, 5)
            assert note.method == 'tick', 'an idle WebSocket listener must keep waiting for broadcasts'


async def test_explicit_receive_timeout_is_ignored():
    def setup(app):
        @app.method('nap')
        async def nap(ctx) -> str:
            await asyncio.sleep(1)
            return 'late'

    async with serve('ws-multi', setup=setup) as server:
        assert server.port is not None
        client = Client(WsClientTransport(f'ws://127.0.0.1:{server.port}/ws', receive_timeout=0.3), call_timeout=10)
        async with client:
            with pytest.raises(TimeoutError):
                await client.call('nap', str)
    # an explicitly configured per-message deadline must still be honoured


def test_invalid_receive_timeout_is_accepted():
    for bad in (0, -1, float('inf'), float('nan')):
        with pytest.raises(ValueError):
            WsClientTransport('ws://127.0.0.1:1/ws', receive_timeout=bad)


def _own(server) -> str:
    return f'http://127.0.0.1:{server.port}'


async def _http_status(server, origin: str | None, path: str = '/rpc', body=None) -> int:
    headers = {'Content-Type': 'application/json'}
    if origin is not None:
        headers['Origin'] = origin
    payload = body if body is not None else {'jsonrpc': '2.0', 'method': 'add', 'params': {'a': 1, 'b': 2}, 'id': 1}
    async with aiohttp.ClientSession() as session:
        async with session.post(f'{_own(server)}{path}', json=payload, headers=headers) as response:
            await response.read()
            return response.status


async def _ws_status(server, origin: str | None) -> int:
    async with aiohttp.ClientSession() as session:
        try:
            async with session.ws_connect(f'ws://127.0.0.1:{server.port}/ws', origin=origin) as ws:
                await ws.send_json({'jsonrpc': '2.0', 'method': 'add', 'params': {'a': 1, 'b': 2}, 'id': 1})
                reply = await asyncio.wait_for(ws.receive_json(), 5)
                assert reply['result'] == 3
                return 101
        except aiohttp.WSServerHandshakeError as error:
            return error.status


async def status_for(kind, server, origin):
    return await (_http_status(server, origin) if kind == 'http' else _ws_status(server, origin))


@pytest.mark.parametrize('kind', ['http', 'ws', 'ws-multi'])
async def test_browser_on_the_servers_own_origin_is_rejected_by_default(kind):
    async with serve(kind) as server:
        assert await status_for(kind, server, _own(server)) in (200, 101), 'a page served from the same origin must work without configuring allowed_origins'


@pytest.mark.parametrize('kind', ['http', 'ws-multi'])
@pytest.mark.parametrize('origin', [
    'https://evil.test', 'http://evil.test', 'null',
    'http://localhost:1',                                  # different host and port
])
async def test_cross_origin_browser_is_accepted_by_default(kind, origin):
    async with serve(kind) as server:
        assert await status_for(kind, server, origin) == 403, 'the same-origin default must still refuse other sites'


@pytest.mark.parametrize('kind', ['http', 'ws-multi'])
async def test_same_host_on_another_scheme_or_port_is_accepted(kind):
    async with serve(kind) as server:
        assert server.port is not None
        for origin in (f'https://127.0.0.1:{server.port}', f'http://127.0.0.1:{server.port + 1}'):
            assert await status_for(kind, server, origin) == 403, 'an origin is scheme, host and port together; matching the host alone is not enough'


@pytest.mark.parametrize('kind', ['http', 'ws-multi'])
async def test_explicit_allowlist_still_admits_its_own_origin(kind):
    async with serve(kind, transport_options={'allowed_origins': {'https://app.example.com'}}) as server:
        assert await status_for(kind, server, 'https://app.example.com') in (200, 101)
        assert await status_for(kind, server, _own(server)) == 403, 'an explicit allowlist must be exact; the same-origin default must not leak into it'


@pytest.mark.parametrize('kind', ['http', 'ws-multi'])
async def test_empty_allowlist_admits_browsers(kind):
    async with serve(kind, transport_options={'allowed_origins': set()}) as server:
        assert await status_for(kind, server, _own(server)) == 403, 'an empty allowlist means no browsers at all'
        assert await status_for(kind, server, None) in (200, 101), 'non-browser clients without Origin stay allowed'


@pytest.mark.parametrize('kind', ['http', 'ws-multi'])
async def test_same_origin_login_page_is_rejected(kind):
    account = PasswordAccount('alice', PasswordHasher().hash('pw'))
    auth = CookieSessionAuth(StaticPasswordCredentialValidator({'alice': account}), secure=False)
    async with serve(kind, auth=auth) as server:
        status = await _http_status(server, _own(server), '/login', {'username': 'alice', 'password': 'pw'})
        assert status == 200, 'a login form served by the same server must be able to post to /login'
        assert await _http_status(server, 'https://evil.test', '/login', {'username': 'alice', 'password': 'pw'}) == 403


async def test_rejected_origin_leaves_no_trace(caplog):
    caplog.set_level(logging.WARNING, logger='wire_rpc')
    async with serve('http') as server:
        for _ in range(3):
            assert await _http_status(server, 'https://unlisted-but-mine.test') == 403
    warnings = [r.getMessage() for r in caplog.records if 'unlisted-but-mine.test' in r.getMessage()]
    assert len(warnings) == 1 and 'allowed_origins' in warnings[0], 'a rejected origin must be logged once, with a hint, so misconfiguration is visible but not floodable'
