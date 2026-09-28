import uuid
import pytest
from wire_rpc.auth.sessions import PostgresSessionStore, RedisSessionStore


pytestmark = [pytest.mark.integration, pytest.mark.timeout(180)]


@pytest.mark.parametrize('stores', ['redis', 'postgres'], indirect=True)
async def test_revocation_in_one_namespace_cannot_destroy_another_tenants_session(stores):
    first, _ = stores
    other: RedisSessionStore | PostgresSessionStore
    namespace = "other-" + uuid.uuid4().hex
    if isinstance(first, RedisSessionStore):
        other = RedisSessionStore(first._client, namespace=namespace)
    else:
        other = PostgresSessionStore(first._pool, namespace=namespace)
        await other.startup()
    token = await first.create('alice')
    isolated = await other.create('alice')
    try:
        assert await other.validate(token) is None, 'a valid token from another tenant must never cross a namespace boundary'
        await first.revoke_all('alice')
        assert await other.validate(isolated) == 'alice', 'account-wide logout in one tenant must not revoke the same principal in another tenant'
    finally:
        await other.destroy(isolated)


@pytest.mark.parametrize('stores', ['redis', 'postgres'], indirect=True)
async def test_full_namespace_cannot_exhaust_another_tenants_session_capacity(stores):
    first, _ = stores
    other: RedisSessionStore | PostgresSessionStore
    namespace = "other-" + uuid.uuid4().hex
    if isinstance(first, RedisSessionStore):
        other = RedisSessionStore(first._client, namespace=namespace, max_sessions=1)
    else:
        other = PostgresSessionStore(first._pool, namespace=namespace, max_sessions=1)
        await other.startup()
    await first.create('alice')
    await first.create('bob')
    isolated = await other.create('charlie')
    try:
        assert await other.validate(isolated) == 'charlie', 'one tenant filling its session quota must not deny login to independent tenants'
    finally:
        await other.destroy(isolated)
