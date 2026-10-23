"""Layer 2: every transport x authenticator pairing, attacked end to end (JSON codec)."""
import asyncio
import secrets
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

import aiohttp
import jwt
import pytest
from argon2 import PasswordHasher

from tests.e2e.harness import Server, make_tls, serve
from wire_rpc import Client, current_request
from wire_rpc.auth import (
    ApiKeyAuth,
    BearerAuth,
    ClientCertificateAuth,
    CookieSessionAuth,
    HmacChallengeAuth,
    HmacChallengeClientAuth,
    TcpTokenAuth,
    TcpTokenClientAuth,
    credential_headers,
)
from wire_rpc.auth.credentials import (
    ApiKeyRecord,
    ApiKeyValidator,
    JwtTokenValidator,
    PasswordAccount,
    SessionTokenValidator,
    StaticPasswordCredentialValidator,
)
from wire_rpc.auth.sessions import InMemorySessionStore
from wire_rpc.auth.shared_secret import SharedSecretAuth
from wire_rpc.client import WireRpcError
from wire_rpc.transports.errors import TransportError

pytestmark = pytest.mark.timeout(30)

# Rejection may surface at connect or at the first call, depending on the transport. These tests
# check that rejection happens and nothing runs; which exception type the client raises is
# checked separately in test_refused_combos.py.
REJECTED = (ConnectionError, WireRpcError, TransportError, aiohttp.ClientError, OSError, EOFError, TimeoutError)

JWT_KEY, JWT_OTHER = 'k' * 48, 'o' * 48
API_SECRET = secrets.token_urlsafe(32)
HMAC_SECRET = secrets.token_bytes(32)
PASSWORD = 'correct horse battery staple'


@dataclass
class Scenario:
    """How to configure the server, and how each kind of client presents itself."""
    server_auth: Any
    mtls: bool
    tls: bool
    good: Callable[[Server], Awaitable[dict]]
    wrong: Callable[[Server], Awaitable[dict]]
    missing: Callable[[Server], Awaitable[dict]]


def _jwt(key: str) -> str:
    now = time.time()
    claims = {'sub': 'alice', 'iss': 'e2e', 'aud': 'rpc', 'iat': now - 1, 'exp': now + 300}
    return jwt.encode(claims, key, algorithm='HS256', headers={'kid': 'k1'})


def _jwt_validator():
    return JwtTokenValidator({'k1': JWT_KEY}, algorithm='HS256', issuer='e2e', audience='rpc')


def _api_key_validator():
    record = ApiKeyRecord.from_secret('alice', API_SECRET)

    async def lookup(key_id: str):
        return record if key_id == 'k1' else None
    return ApiKeyValidator(lookup)


def _const(value: dict):
    async def build(server: Server) -> dict:
        return dict(value)
    return build


async def _scenario(name: str, tls) -> Scenario:
    plain = {'ssl_context': tls.client}
    if name == 'tcp-token-apikey':
        return Scenario(TcpTokenAuth(_api_key_validator()), False, True,
                        _const({**plain, 'auth': TcpTokenClientAuth(f'k1.{API_SECRET}')}),
                        _const({**plain, 'auth': TcpTokenClientAuth(f'k1.{secrets.token_urlsafe(32)}')}),
                        _const(plain))
    if name == 'tcp-token-jwt':
        return Scenario(TcpTokenAuth(_jwt_validator()), False, True,
                        _const({**plain, 'auth': TcpTokenClientAuth(_jwt(JWT_KEY))}),
                        _const({**plain, 'auth': TcpTokenClientAuth(_jwt(JWT_OTHER))}),
                        _const(plain))
    if name == 'tcp-token-session':
        store = InMemorySessionStore()
        token = await store.create('alice')
        return Scenario(TcpTokenAuth(SessionTokenValidator(store)), False, True,
                        _const({**plain, 'auth': TcpTokenClientAuth(token)}),
                        _const({**plain, 'auth': TcpTokenClientAuth(secrets.token_urlsafe(32))}),
                        _const(plain))
    if name == 'tcp-hmac':
        return Scenario(HmacChallengeAuth({'k1': ('alice', HMAC_SECRET)}), False, True,
                        _const({**plain, 'auth': HmacChallengeClientAuth('k1', HMAC_SECRET)}),
                        _const({**plain, 'auth': HmacChallengeClientAuth('k1', secrets.token_bytes(32))}),
                        _const(plain))
    if name in ('tcp-cert', 'web-cert'):
        return Scenario(ClientCertificateAuth({tls.client_fingerprint: 'alice'}), True, True,
                        _const({'ssl_context': tls.client_with_cert}),
                        _const({'ssl_context': tls.client}),  # trusted server, but no client certificate
                        _const({'ssl_context': tls.client}))
    if name == 'tcp-shared-secret':
        secret = secrets.token_urlsafe(32)
        auth = SharedSecretAuth(secret, principal='alice')
        return Scenario(auth, False, False,
                        _const({'auth': auth}),
                        _const({'auth': SharedSecretAuth(secrets.token_urlsafe(32), principal='alice')}),
                        _const({}))
    if name == 'web-bearer-jwt':
        return Scenario(BearerAuth(_jwt_validator()), False, True,
                        _const({**plain, 'headers': credential_headers(bearer=_jwt(JWT_KEY))}),
                        _const({**plain, 'headers': credential_headers(bearer=_jwt(JWT_OTHER))}),
                        _const(plain))
    if name == 'web-bearer-session':
        store = InMemorySessionStore()
        token = await store.create('alice')
        return Scenario(BearerAuth(SessionTokenValidator(store)), False, True,
                        _const({**plain, 'headers': credential_headers(bearer=token)}),
                        _const({**plain, 'headers': credential_headers(bearer=secrets.token_urlsafe(32))}),
                        _const(plain))
    if name == 'web-apikey':
        return Scenario(ApiKeyAuth(_api_key_validator()), False, True,
                        _const({**plain, 'headers': credential_headers(api_key=f'k1.{API_SECRET}')}),
                        _const({**plain, 'headers': credential_headers(api_key=f'k1.{secrets.token_urlsafe(32)}')}),
                        _const(plain))
    if name == 'web-cookie':
        account = PasswordAccount('alice', PasswordHasher().hash(PASSWORD))
        auth = CookieSessionAuth(StaticPasswordCredentialValidator({'alice': account}), InMemorySessionStore())

        def login(password: str):
            async def build(server: Server) -> dict:
                jar = aiohttp.CookieJar(unsafe=True)
                async with aiohttp.ClientSession(cookie_jar=jar) as session:
                    async with session.post(f'{server.base_url}/login', ssl=tls.client,
                                            json={'username': 'alice', 'password': password}) as response:
                        await response.read()
                return {**plain, 'cookie_jar': jar}
            return build
        return Scenario(auth, False, True, login(PASSWORD), login('wrong password'), _const(plain))
    raise AssertionError(name)


TCP_SCHEMES = ['tcp-token-apikey', 'tcp-token-jwt', 'tcp-token-session', 'tcp-hmac', 'tcp-cert', 'tcp-shared-secret']
WEB_SCHEMES = ['web-bearer-jwt', 'web-bearer-session', 'web-apikey', 'web-cookie', 'web-cert']
COMBOS = ([pytest.param(kind, scheme, id=f'{kind}-{scheme}') for kind in ('tcp', 'tcp-multi') for scheme in TCP_SCHEMES]
          + [pytest.param('http', scheme, id=f'http-{scheme}') for scheme in WEB_SCHEMES]
          + [pytest.param(kind, scheme, id=f'{kind}-{scheme}') for kind in ('ws', 'ws-multi') for scheme in WEB_SCHEMES])


def _recorder(touched: list):
    def setup(app):
        @app.method('touch')
        async def touch(ctx) -> str | None:
            touched.append(current_request().principal)
            return current_request().principal
    return setup


async def _attempt(server: Server, options: dict) -> str:
    client = Client(server.wire(**options), call_timeout=5)
    try:
        await client.connect()
        return await client.call('touch', str)
    finally:
        await asyncio.wait_for(client.close(), 5)


def _serve(kind, scenario: Scenario, tls, touched):
    ssl_context = (tls.server_mtls if scenario.mtls else tls.server) if scenario.tls else None
    return serve(kind, auth=scenario.server_auth, ssl_context=ssl_context, setup=_recorder(touched))


@pytest.fixture(scope='module')
def tls(tmp_path_factory):
    return make_tls(tmp_path_factory.mktemp('tls'))


@pytest.mark.parametrize('kind,scheme', COMBOS)
async def test_valid_credentials_lose_their_identity(kind, scheme, tls):
    scenario = await _scenario(scheme, tls)
    touched: list = []
    async with _serve(kind, scenario, tls, touched) as server:
        assert await _attempt(server, await scenario.good(server)) == 'alice', 'the authenticated principal must reach the handler unchanged'
        assert touched == ['alice']


@pytest.mark.parametrize('kind,scheme', COMBOS)
async def test_wrong_credentials_reach_a_handler(kind, scheme, tls):
    scenario = await _scenario(scheme, tls)
    touched: list = []
    async with _serve(kind, scenario, tls, touched) as server:
        with pytest.raises(REJECTED):
            await asyncio.wait_for(_attempt(server, await scenario.wrong(server)), 8)
        assert touched == [], 'a client with wrong credentials must never execute a handler'
        assert await _attempt(server, await scenario.good(server)) == 'alice', 'a rejected client must not lock the server against a legitimate one'


@pytest.mark.parametrize('kind,scheme', COMBOS)
async def test_missing_credentials_reach_a_handler(kind, scheme, tls):
    scenario = await _scenario(scheme, tls)
    touched: list = []
    async with _serve(kind, scenario, tls, touched) as server:
        with pytest.raises(REJECTED):
            await asyncio.wait_for(_attempt(server, await scenario.missing(server)), 8)
        assert touched == [], 'an unauthenticated client must never execute a handler'
        assert await _attempt(server, await scenario.good(server)) == 'alice', 'an unauthenticated attempt must not lock the server against a legitimate one'


@pytest.mark.parametrize('kind,scheme', [p for p in COMBOS if p.values[1] in ('tcp-token-jwt', 'web-bearer-jwt')])
async def test_params_can_spoof_the_principal(kind, scheme, tls):
    scenario = await _scenario(scheme, tls)
    touched: list = []
    async with _serve(kind, scenario, tls, touched) as server:
        client = Client(server.wire(**await scenario.good(server)), call_timeout=5)
        async with client:
            assert await client.call('whoami', str, {'principal': 'admin', 'user': 'admin'}) == 'alice', 'identity must come from authentication, never from caller-supplied params'


@pytest.mark.parametrize('kind', ['stdio'])
async def test_trusted_principal_is_lost_over_stdio(kind):
    async with serve(kind, principal='parent-process') as server:
        async with Client(server.wire(), call_timeout=5) as client:
            assert await client.call('whoami', str) == 'parent-process', 'the configured trusted principal must reach handlers in the child process'
