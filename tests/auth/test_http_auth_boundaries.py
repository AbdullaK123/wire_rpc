import asyncio
from types import SimpleNamespace
from multidict import CIMultiDict
from unittest.mock import AsyncMock, Mock
import pytest
from aiohttp import web
from wire_rpc.auth import BearerAuth, ApiKeyAuth, CookieSessionAuth, AuthUnavailableError
from wire_rpc.auth.sessions import InMemorySessionStore
from wire_rpc.transports.http import HttpServerTransport
from wire_rpc.transports.websocket import MulticastWsServerTransport


@pytest.mark.parametrize('headers', [[], [('Authorization','Bearer a'),('Authorization','Bearer b')],
    [('Authorization','Basic abc')], [('Authorization','Bearer  abc')], [('Authorization','Bearer abc\r\nInjected: yes')], [('Authorization','Bearer')]])
async def test_ambiguous_or_malformed_authorization_headers_never_reach_validation(headers):
    validator = Mock(validate=AsyncMock(return_value='alice'))
    auth = BearerAuth(validator)
    request = Mock(secure=True, headers=CIMultiDict(headers))
    assert await auth.verify(request) is None, 'ambiguous credential parsing must not let proxies and application code disagree about the authenticated caller'
    assert validator.validate.await_count == 0, 'invalid authorization headers must fail before consuming credential-backend capacity'


@pytest.mark.parametrize('factory,header', [(BearerAuth,'Authorization'), (ApiKeyAuth,'X-API-Key')])
async def test_plaintext_requests_cannot_use_network_bearer_credentials(factory, header):
    validator = Mock(validate=AsyncMock(return_value='alice'))
    request = Mock(secure=False, headers=CIMultiDict({header:'Bearer abc' if header=='Authorization' else 'abc'}))
    assert await factory(validator).verify(request) is None, 'network credential authenticators must enforce their default TLS policy'
    assert validator.validate.await_count == 0, 'plaintext credentials must be rejected before backend access'


@pytest.mark.parametrize('transport_factory', [HttpServerTransport, MulticastWsServerTransport])
async def test_verify_only_authenticator_does_not_mount_broken_login_routes(transport_factory):
    validator = Mock(validate=AsyncMock(return_value='alice'))
    transport = transport_factory('127.0.0.1', 0, auth=BearerAuth(validator))
    try:
        await transport.connect()
        runner = transport._runner
        assert runner is not None, 'the auth route test must inspect the actual bound application'
        paths = {route.resource.canonical for route in runner.app.router.routes()}
        assert '/login' not in paths and '/logout' not in paths, 'noninteractive auth must not expose routes that call missing login/logout methods'
    finally:
        await transport.close()


@pytest.mark.parametrize('body', [[], None, 5, 'alice'])
async def test_cookie_login_rejects_non_object_bodies_before_credential_backend(body):
    validator = Mock(validate=AsyncMock(return_value='alice'))
    auth = CookieSessionAuth(validator)
    request = Mock(json=AsyncMock(return_value=body))
    response = await auth.login(request)
    assert response.status == 400 and validator.validate.await_count == 0, 'JSON arrays and scalars must not reach validators expecting credential mappings'


async def test_cookie_login_store_failure_returns_safe_unavailable_without_setting_cookie():
    validator = Mock(validate=AsyncMock(return_value='alice'))
    store = Mock(create=AsyncMock(side_effect=AuthUnavailableError('VERY_SECRET')))
    auth = CookieSessionAuth(validator, store)
    response = await auth.login(Mock(json=AsyncMock(return_value={})))
    assert response.status == 503 and not response.cookies, 'a failed canonical session write must not issue a cookie that appears authenticated'
    assert response.text is not None and 'VERY_SECRET' not in response.text, 'session backend exceptions must not leak credential-bearing diagnostics'


async def test_revoked_cookie_cannot_authorize_a_later_request():
    store = InMemorySessionStore()
    token = await store.create('alice')
    auth = CookieSessionAuth(Mock(validate=AsyncMock()), store)
    await store.destroy(token)
    assert await auth.verify(Mock(cookies={'session':token}, headers=CIMultiDict())) is None, 'logout must take effect on subsequent HTTP and WebSocket message authorization checks'


async def test_duplicate_session_cookies_cannot_select_a_different_authenticated_user():
    store = InMemorySessionStore()
    alice, bob = await store.create('alice'), await store.create('bob')
    auth = CookieSessionAuth(Mock(validate=AsyncMock()), store)
    request = Mock(cookies={'session':bob}, headers=CIMultiDict({'Cookie':f'session={alice}; session={bob}'}))
    assert await auth.verify(request) is None, 'duplicate session cookies must fail closed rather than let different HTTP parsers select different identities'


async def test_cookie_login_does_not_read_passwords_on_plaintext_connections():
    request = Mock(secure=False, json=AsyncMock(return_value={'username':'alice','password':'secret'}))
    auth = CookieSessionAuth(Mock(validate=AsyncMock(return_value='alice')))
    response = await auth.login(request)
    assert response.status == 403 and request.json.await_count == 0, 'the default cookie login policy must refuse plaintext before credential parsing and password verification'


async def test_cookie_logout_backend_failure_does_not_claim_success_or_clear_the_cookie():
    store = Mock(destroy=AsyncMock(side_effect=AuthUnavailableError('VERY_SECRET')))
    auth = CookieSessionAuth(Mock(validate=AsyncMock()), store)
    request = Mock(secure=True, cookies={'session':'token'}, headers=CIMultiDict({'Cookie':'session=token'}))
    response = await auth.logout(request)
    assert response.status == 503 and not response.cookies, 'failed server-side revocation must not be presented as a successful logout while the stolen token still works'
    assert response.text is not None and 'VERY_SECRET' not in response.text, 'revocation errors must not expose backend diagnostics'
