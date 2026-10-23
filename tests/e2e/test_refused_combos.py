"""Combinations that must be refused, or fail cleanly, rather than half-work."""
import asyncio
from unittest.mock import AsyncMock

import aiohttp
import pytest

from tests.e2e.harness import make_tls, serve
from wire_rpc import App, Client, MulticastApp
from wire_rpc.auth import (
    ApiKeyAuth,
    BearerAuth,
    HmacChallengeAuth,
    HmacChallengeClientAuth,
    TcpTokenAuth,
    TcpTokenClientAuth,
    credential_headers,
)
from wire_rpc.client import WireRpcError
from wire_rpc.codecs.msgspec import MsgSpecJsonCodec, MsgSpecMsgPackCodec
from wire_rpc.transports.errors import TransportError
from wire_rpc.transports.http import HttpClientTransport, HttpServerTransport
from wire_rpc.transports.tcp import TcpClientTransport
from wire_rpc.transports.websocket import (
    MulticastWsServerTransport,
    WsClientTransport,
    WsServerTransport,
)

pytestmark = pytest.mark.timeout(30)

# What Client.call documents for a closed or broken channel.
DOCUMENTED = (ConnectionError, WireRpcError, TimeoutError)


@pytest.fixture(scope='module')
def tls(tmp_path_factory):
    return make_tls(tmp_path_factory.mktemp('tls'))


@pytest.mark.parametrize('build', [
    pytest.param(lambda: App(HttpServerTransport('127.0.0.1', 0), codec=MsgSpecMsgPackCodec()), id='http-server'),
    pytest.param(lambda: Client(HttpClientTransport('http://127.0.0.1:1/rpc'), codec=MsgSpecMsgPackCodec()), id='http-client'),
    pytest.param(lambda: App(WsServerTransport('127.0.0.1', 0), codec=MsgSpecMsgPackCodec()), id='ws-server'),
    pytest.param(lambda: MulticastApp(MulticastWsServerTransport('127.0.0.1', 0), codec=MsgSpecMsgPackCodec()), id='ws-multi-server'),
    pytest.param(lambda: Client(WsClientTransport('ws://127.0.0.1:1/ws'), codec=MsgSpecMsgPackCodec()), id='ws-client'),
])
def test_binary_codec_accepted_on_text_transport(build):
    with pytest.raises(ValueError):
        build()
    # MessagePack bytes are not valid UTF-8 text frames; accepting the pairing fails at runtime instead


@pytest.mark.parametrize('scheme', ['token', 'hmac'])
async def test_tcp_credentials_accepted_without_tls(scheme):
    validator = AsyncMock(validate=AsyncMock(return_value='alice'))
    server_auth = TcpTokenAuth(validator) if scheme == 'token' else HmacChallengeAuth({'k': ('alice', b's' * 32)})
    client_auth = (TcpTokenClientAuth('k.' + 'x' * 40, require_tls=False) if scheme == 'token'
                   else HmacChallengeClientAuth('k', b's' * 32, require_tls=False))
    async with serve('tcp-multi', auth=server_auth) as server:
        with pytest.raises(Exception):
            async with Client(server.wire(auth=client_auth), call_timeout=5) as client:
                await client.call('whoami', str)
    validator.validate.assert_not_called()
    # a TLS-requiring server must refuse before it even looks at credentials sent in the clear


@pytest.mark.parametrize('scheme', ['token', 'hmac'])
async def test_tcp_client_sends_credentials_without_tls(scheme):
    validator = AsyncMock(validate=AsyncMock(return_value='alice'))
    server_auth = (TcpTokenAuth(validator, require_tls=False) if scheme == 'token'
                   else HmacChallengeAuth({'k': ('alice', b's' * 32)}, require_tls=False))
    client_auth = TcpTokenClientAuth('k.' + 'x' * 40) if scheme == 'token' else HmacChallengeClientAuth('k', b's' * 32)
    async with serve('tcp-multi', auth=server_auth) as server:
        with pytest.raises(PermissionError):
            await Client(server.wire(auth=client_auth), call_timeout=5).connect()
    validator.validate.assert_not_called()
    # the client must refuse to put a secret on a plaintext socket even if the server would accept it


@pytest.mark.parametrize('kind', ['http', 'ws'])
@pytest.mark.parametrize('scheme', ['bearer', 'api-key'])
async def test_web_credentials_accepted_without_tls(kind, scheme):
    validator = AsyncMock(validate=AsyncMock(return_value='alice'))
    auth = BearerAuth(validator) if scheme == 'bearer' else ApiKeyAuth(validator)
    headers = credential_headers(bearer='t' * 40) if scheme == 'bearer' else credential_headers(api_key='k.' + 't' * 40)
    async with serve(kind, auth=auth) as server:
        with pytest.raises((ConnectionError, aiohttp.ClientError)):
            async with Client(server.wire(headers=headers), call_timeout=5) as client:
                await client.call('whoami', str)
    validator.validate.assert_not_called()


@pytest.mark.parametrize('build', [
    pytest.param(lambda: HttpClientTransport('http://127.0.0.1:1/rpc', headers=credential_headers(bearer='t' * 40)), id='http-headers'),
    pytest.param(lambda: WsClientTransport('ws://127.0.0.1:1/ws', headers=credential_headers(bearer='t' * 40)), id='ws-headers'),
    pytest.param(lambda: HttpClientTransport('http://127.0.0.1:1/rpc', cookie_jar=aiohttp.CookieJar()), id='http-cookies'),
    pytest.param(lambda: HttpClientTransport('https://user:pass@127.0.0.1:1/rpc'), id='url-userinfo'),
])
async def test_client_credentials_allowed_over_plaintext(build):
    with pytest.raises(ValueError):
        build()


@pytest.mark.parametrize('kind', ['tcp-multi', 'stdio', 'thread'])
async def test_codec_mismatch_hangs_or_leaks_internal_errors(kind):
    async with serve(kind, 'json') as server:
        client = Client(server.wire(), codec=MsgSpecMsgPackCodec(), call_timeout=5)
        await client.connect()
        try:
            with pytest.raises(DOCUMENTED):
                await asyncio.wait_for(client.call('add', int, {'a': 1, 'b': 2}), 8)
        finally:
            await client.close()
    # Client.call documents ConnectionError for a malformed reply; a raw codec error is an undocumented failure mode


@pytest.mark.parametrize('kind', ['tcp-multi', 'http', 'ws-multi'])
async def test_json_client_cannot_talk_to_pydantic_server(kind):
    async with serve(kind, 'pydantic') as server:
        async with Client(server.wire(), codec=MsgSpecJsonCodec(), call_timeout=5) as client:
            assert await client.call('add', int, {'a': 1, 'b': 2}) == 3, 'both codecs speak JSON and must interoperate'


@pytest.mark.parametrize('kind', ['http', 'ws-multi'])
async def test_tcp_client_against_web_server_hangs(kind):
    async with serve(kind) as server:
        assert server.port is not None
        client = Client(TcpClientTransport(host='127.0.0.1', port=server.port, keep_alive=None, read_timeout=3), call_timeout=5)
        with pytest.raises((*DOCUMENTED, TransportError, EOFError, OSError)):
            await client.connect()
            await asyncio.wait_for(client.call('add', int, {'a': 1, 'b': 2}), 8)
        await client.close()
    # pointing the wrong client at a port must fail quickly, never hang or misread HTTP as a frame


async def test_web_client_against_tcp_server_hangs():
    async with serve('tcp-multi') as server:
        client = Client(WsClientTransport(f'ws://127.0.0.1:{server.port}/ws', connect_timeout=3), call_timeout=5)
        with pytest.raises((*DOCUMENTED, aiohttp.ClientError, OSError)):
            await asyncio.wait_for(client.connect(), 8)
        await client.close()
        async with Client(server.wire(), call_timeout=5) as real:
            assert await real.call('add', int, {'a': 1, 'b': 1}) == 2, 'a confused HTTP client must not wedge the TCP server'


async def test_plaintext_client_against_tls_server_hangs(tls):
    async with serve('tcp-multi', ssl_context=tls.server) as server:
        client = Client(server.wire(), call_timeout=5)
        with pytest.raises((*DOCUMENTED, TransportError, EOFError, OSError)):
            await client.connect()
            await asyncio.wait_for(client.call('add', int, {'a': 1, 'b': 2}), 8)
        await client.close()
        async with Client(server.wire(ssl_context=tls.client), call_timeout=5) as real:
            assert await real.call('add', int, {'a': 1, 'b': 1}) == 2, 'a plaintext probe must not wedge the TLS listener'
