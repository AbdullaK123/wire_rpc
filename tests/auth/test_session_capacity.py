import pytest
from wire_rpc.auth.sessions import memory


async def test_expired_unvisited_sessions_do_not_permanently_exhaust_capacity(monkeypatch):
    now=[100.0]; monkeypatch.setattr(memory.time,'monotonic',lambda:now[0])
    store=memory.InMemorySessionStore(ttl=5,max_sessions=1)
    old=await store.create('alice')
    with pytest.raises(memory.SessionCapacityError):
        await store.create('bob')
    now[0]=105.0
    new=await store.create('bob')
    assert await store.validate(old) is None, 'expired sessions must cease authorizing exactly at their expiration boundary'
    assert await store.validate(new) == 'bob', 'new login capacity must recover without an expired user revisiting their session'


async def test_login_logout_churn_cannot_grow_expiry_bookkeeping_without_bound():
    store=memory.InMemorySessionStore(max_sessions=2)
    for _ in range(100):
        key=await store.create('alice'); await store.destroy(key)
    assert len(store._expiry) <= 2*store._max_sessions, 'logout must not leave an unbounded trail of expiration tombstones'
