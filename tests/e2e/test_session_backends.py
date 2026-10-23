"""Layer 3: every session store behind cookie login (HTTP) and session tokens (TCP), end to end."""
import asyncio
import uuid
from collections.abc import Awaitable, Callable

import aiohttp
import pytest
from argon2 import PasswordHasher
from yarl import URL

from tests.e2e.harness import make_tls, serve
from wire_rpc import Client, current_request
from wire_rpc.auth import CookieSessionAuth, TcpTokenAuth, TcpTokenClientAuth
from wire_rpc.auth.credentials import (
    PasswordAccount,
    SessionTokenValidator,
    StaticPasswordCredentialValidator,
)
from wire_rpc.auth.sessions import (
    InMemorySessionStore,
    PostgresSessionStore,
    RedisSessionStore,
    SQLiteSessionStore,
)
from wire_rpc.client import WireRpcError
from wire_rpc.codecs import CodecError
from wire_rpc.transports.errors import TransportError

pytestmark = pytest.mark.timeout(60)

DENIED = (WireRpcError, ConnectionError, TransportError, CodecError, aiohttp.ClientError, OSError, EOFError)
PASSWORD = 'correct horse battery staple'


@pytest.fixture(scope='module')
def tls(tmp_path_factory):
    return make_tls(tmp_path_factory.mktemp('tls'))


@pytest.fixture(params=[
    'memory', 'sqlite',
    pytest.param('redis', marks=[pytest.mark.integration, pytest.mark.timeout(180)]),
    pytest.param('postgres', marks=[pytest.mark.integration, pytest.mark.timeout(180)]),
])
async def make_store(request, tmp_path):
    """Build a started store of the parametrized backend, isolated by namespace or file."""
    kind = request.param
    cleanup: list[Callable[[], Awaitable[object]]] = []

    async def make(ttl: float = 86400):
        namespace = 'e2e-' + uuid.uuid4().hex
        if kind == 'memory':
            return InMemorySessionStore(ttl)
        if kind == 'sqlite':
            store = SQLiteSessionStore(str(tmp_path / f'{namespace}.sqlite'), ttl)
        elif kind == 'redis':
            from redis.asyncio import Redis
            client = Redis.from_url(request.getfixturevalue('redis_url'), socket_timeout=5)
            cleanup.append(lambda: client.aclose())
            store = RedisSessionStore(client, ttl, namespace=namespace)
        else:
            import asyncpg
            pool = await asyncpg.create_pool(request.getfixturevalue('postgres_dsn'), min_size=1, max_size=4)
            cleanup.append(lambda: pool.close())
            store = PostgresSessionStore(pool, ttl, namespace=namespace)
        if hasattr(store, 'startup'):
            await store.startup()
        return store

    yield make
    for close in reversed(cleanup):
        await close()


def _whoami_strict(touched):
    def setup(app):
        @app.method('touch')
        async def touch(ctx) -> str | None:
            touched.append(current_request().principal)
            return current_request().principal
    return setup


async def _login(server, tls, password=PASSWORD) -> aiohttp.CookieJar:
    jar = aiohttp.CookieJar(unsafe=True)
    async with aiohttp.ClientSession(cookie_jar=jar) as session:
        async with session.post(f'{server.base_url}/login', ssl=tls.client,
                                json={'username': 'alice', 'password': password}) as response:
            await response.read()
    return jar


def _cookie_auth(store):
    account = PasswordAccount('alice', PasswordHasher().hash(PASSWORD))
    return CookieSessionAuth(StaticPasswordCredentialValidator({'alice': account}), store)


async def test_cookie_survives_logout(make_store, tls):
    store = await make_store()
    touched: list = []
    async with serve('http', auth=_cookie_auth(store), ssl_context=tls.server, setup=_whoami_strict(touched)) as server:
        jar = await _login(server, tls)
        stolen = {cookie.key: cookie.value for cookie in jar}
        assert stolen.get('session'), 'login must set the session cookie'
        async with Client(server.wire(cookie_jar=jar, ssl_context=tls.client), call_timeout=5) as client:
            assert await client.call('touch', str) == 'alice'
        async with aiohttp.ClientSession(cookie_jar=jar) as session:
            async with session.post(f'{server.base_url}/logout', ssl=tls.client) as response:
                assert response.status == 200
        replay = aiohttp.CookieJar(unsafe=True)
        replay.update_cookies(stolen, response_url=URL(server.base_url))
        with pytest.raises(DENIED):
            async with Client(server.wire(cookie_jar=replay, ssl_context=tls.client), call_timeout=5) as client:
                await client.call('touch', str)
        assert touched == ['alice'], 'a cookie captured before logout must be dead after logout'


async def test_failed_login_still_issues_a_session(make_store, tls):
    store = await make_store()
    touched: list = []
    async with serve('http', auth=_cookie_auth(store), ssl_context=tls.server, setup=_whoami_strict(touched)) as server:
        jar = await _login(server, tls, password='guess')
        assert not [cookie for cookie in jar if cookie.key == 'session'], 'a failed login must not hand out a session cookie'
        with pytest.raises(DENIED):
            async with Client(server.wire(cookie_jar=jar, ssl_context=tls.client), call_timeout=5) as client:
                await client.call('touch', str)
        assert touched == []


async def test_session_token_revocation_ignored_on_open_tcp_connection(make_store, tls):
    store = await make_store()
    token = await store.create('alice')
    touched: list = []
    async with serve('tcp-multi', auth=TcpTokenAuth(SessionTokenValidator(store)), ssl_context=tls.server,
                     setup=_whoami_strict(touched)) as server:
        client = Client(server.wire(auth=TcpTokenClientAuth(token), ssl_context=tls.client), call_timeout=5)
        async with client:
            assert await client.call('touch', str) == 'alice'
            assert await store.revoke_all('alice') == 1
            with pytest.raises(DENIED):
                await client.call('touch', str)
        assert touched == ['alice'], 'revoking a session must stop the next call on an already-open connection'


async def test_rotated_session_token_keeps_working(make_store, tls):
    store = await make_store()
    old = await store.create('alice')
    touched: list = []
    async with serve('tcp-multi', auth=TcpTokenAuth(SessionTokenValidator(store)), ssl_context=tls.server,
                     setup=_whoami_strict(touched)) as server:
        new = await store.rotate(old)
        assert new and new != old
        with pytest.raises(DENIED):
            async with Client(server.wire(auth=TcpTokenClientAuth(old), ssl_context=tls.client), call_timeout=5) as client:
                await client.call('touch', str)
        async with Client(server.wire(auth=TcpTokenClientAuth(new), ssl_context=tls.client), call_timeout=5) as client:
            assert await client.call('touch', str) == 'alice'
        assert touched == ['alice'], 'after rotation only the new token may authenticate'


async def test_expired_session_token_still_authenticates(make_store, tls):
    store = await make_store(ttl=1)
    token = await store.create('alice')
    touched: list = []
    async with serve('tcp-multi', auth=TcpTokenAuth(SessionTokenValidator(store)), ssl_context=tls.server,
                     setup=_whoami_strict(touched)) as server:
        await asyncio.sleep(1.3)
        with pytest.raises(DENIED):
            async with Client(server.wire(auth=TcpTokenClientAuth(token), ssl_context=tls.client), call_timeout=5) as client:
                await client.call('touch', str)
        assert touched == [], 'a session past its ttl must not authenticate a new connection'
