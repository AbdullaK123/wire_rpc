import json
import time
import pytest
from wire_rpc.auth.sessions import memory
from wire_rpc.auth.errors import AuthUnavailableError


async def expire(store, token):
    key = store._digest(token)
    from wire_rpc.auth.sessions import InMemorySessionStore, SQLiteSessionStore, RedisSessionStore
    if isinstance(store, InMemorySessionStore):
        value, _ = store._sessions[key]
        store._sessions[key] = (value, 0)
        import heapq
        heapq.heappush(store._expiry, (0, key))
    elif isinstance(store, SQLiteSessionStore):
        import sqlite3
        with sqlite3.connect(store._path) as db:
            db.execute('UPDATE wire_sessions SET expires=0 WHERE digest=?', (key,))
    elif isinstance(store, RedisSessionStore):
        await store._client.zadd(store._keys[1], {key: 0})
    else:
        await store._pool.execute('UPDATE wire_rpc_sessions SET expires=0 WHERE namespace=$1 AND digest=$2', store._namespace, key)


async def test_expired_session_cannot_be_used_or_renewed_by_another_worker(stores):
    first, second = stores
    token = await first.create('alice')
    await expire(first, token)
    assert await second.validate(token) is None, 'server-side expiration must reject old credentials even when their cookie remains present'
    assert await second.rotate(token) is None, 'rotation must not convert expired credentials into a fresh authenticated session'


async def test_rotating_a_session_does_not_extend_its_absolute_lifetime(monkeypatch):
    now = [10.0]
    monkeypatch.setattr(memory.time, 'monotonic', lambda: now[0])
    store = memory.InMemorySessionStore(ttl=10)
    old = await store.create('alice')
    now[0] = 19.0
    new = await store.rotate(old)
    assert new is not None, 'a live session must be eligible for the boundary rotation under test'
    now[0] = 20.0
    assert await store.validate(new) is None, 'repeated token rotation must not keep stolen credentials alive beyond the original deadline'


async def test_session_backend_exception_never_authorizes_or_leaks_connection_secrets(monkeypatch):
    store = memory.InMemorySessionStore()
    token = await store.create('alice')
    async def fail(*args):
        raise RuntimeError('postgres://root:SUPER_SECRET@internal')
    monkeypatch.setattr(store, '_execute', fail)
    with pytest.raises(AuthUnavailableError) as error:
        await store.validate(token)
    assert 'SUPER_SECRET' not in str(error.value), 'backend outage details must not expose credentials through authentication errors'


async def test_memory_store_does_not_retain_raw_bearer_tokens():
    store = memory.InMemorySessionStore()
    token = await store.create('alice')
    assert token not in repr(store._sessions) + repr(store._expiry), 'a read-only session-store disclosure must not reveal directly reusable bearer credentials'


async def test_corrupt_session_payload_cannot_be_treated_as_authenticated_identity(monkeypatch):
    store = memory.InMemorySessionStore()
    token = await store.create('alice')
    key = store._digest(token)
    _, expiry = store._sessions[key]
    store._sessions[key] = ('{"principal":"alice","payload":null}',expiry)
    with pytest.raises(AuthUnavailableError):
        await store.validate(token)


@pytest.mark.parametrize('stores', ['redis'], indirect=True)
async def test_redis_record_without_expiry_index_never_becomes_a_permanent_session(stores):
    from wire_rpc.auth.sessions import RedisSessionStore
    first, _ = stores
    assert isinstance(first, RedisSessionStore), "this corruption case must exercise the Redis expiry index"
    token = await first.create('alice')
    await first._client.zrem(first._keys[1],first._digest(token))
    assert await first.validate(token) is None, 'a missing Redis expiry index must deny authentication rather than silently creating a nonexpiring session'
