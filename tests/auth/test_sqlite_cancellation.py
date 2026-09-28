import asyncio
import pytest
from wire_rpc.auth.sessions import SQLiteSessionStore
from wire_rpc.auth.errors import AuthUnavailableError


async def test_cancelled_sqlite_request_cannot_admit_unbounded_background_transactions(tmp_path, monkeypatch):
    store = SQLiteSessionStore(str(tmp_path/'sessions.sqlite'), max_concurrency=1)
    await store.startup()
    entered, release, completed = asyncio.Event(), asyncio.Event(), asyncio.Event()
    workers = []
    async def controlled_thread(*args):
        workers.append(asyncio.current_task())
        entered.set()
        await release.wait()
    monkeypatch.setattr(asyncio, 'to_thread', controlled_thread)
    request = asyncio.create_task(store.create('alice'))
    await entered.wait(); request.cancel()
    try:
        with pytest.raises(asyncio.CancelledError):
            await request
        with pytest.raises(AuthUnavailableError):
            await store.create('bob')
        assert store._active == 1, 'disconnecting login clients must not free capacity while their SQLite transaction still owns background resources'
    finally:
        worker = workers[0]
        assert worker is not None, 'SQLite operations must retain an owned worker task until their transaction completes'
        worker.add_done_callback(lambda _: completed.set())
        release.set(); await completed.wait()
        await store.shutdown()
    assert store._active == 0, 'a cancelled session operation must release worker admission exactly once after completion'
