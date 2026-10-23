import asyncio
import threading

import pytest

from wire_rpc import App, Client
from wire_rpc.client import WireRpcError
from wire_rpc.transports.thread import ThreadTransport


class LoopThread:
    """Runs one endpoint on its own thread and event loop, like a real peer."""

    def __init__(self):
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self.loop.run_forever, daemon=True)
        self.thread.start()

    def run(self, coro, timeout=5.0):
        return asyncio.run_coroutine_threadsafe(coro, self.loop).result(timeout)

    def submit(self, coro):
        return asyncio.run_coroutine_threadsafe(coro, self.loop)

    def stop(self):
        if self.loop.is_closed():
            return
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join(5)
        self.loop.close()


@pytest.fixture
def peer_thread():
    worker = LoopThread()
    yield worker
    worker.stop()


async def test_send_to_peer_that_never_connects_hangs_or_drops():
    a, _b = ThreadTransport.pair(connect_timeout=0.1)
    await a.connect()
    with pytest.raises(ConnectionError):
        await asyncio.wait_for(a.send(b'{"jsonrpc":"2.0","method":"ping","id":1}'), 2.0)
    # silent success means the caller waits the full call timeout for a reply that can never come


async def test_message_crosses_threads_intact(peer_thread):
    a, b = ThreadTransport.pair()
    await a.connect()
    peer_thread.run(b.connect())
    await a.send(b"ping")
    received = peer_thread.run(b.recv())
    assert received == b"ping", "bytes sent on one thread must arrive unchanged on the other or every RPC is corrupt"


async def test_burst_of_messages_arrives_out_of_order(peer_thread):
    a, b = ThreadTransport.pair()
    await a.connect()
    peer_thread.run(b.connect())
    sent = [str(i).encode() for i in range(200)]  # stays under the default inbox bound; nobody reads until all are sent
    for message in sent:
        await a.send(message)

    async def drain():
        return [await b.recv() for _ in sent]

    assert peer_thread.run(drain()) == sent, "reordering breaks request/response pairing for callers that pipeline"


async def test_mutating_buffer_after_send_corrupts_delivered_message(peer_thread):
    a, b = ThreadTransport.pair()
    await a.connect()
    peer_thread.run(b.connect())
    buffer = bytearray(b'{"id":1}')
    await a.send(buffer)  # type: ignore[arg-type]  # callers reuse buffers; the transport must not alias them
    buffer[:] = b'{"id":2}'
    received = peer_thread.run(b.recv())
    assert received == b'{"id":1}', "the receiver saw bytes the sender wrote after send() returned — responses land on the wrong request"
    assert isinstance(received, bytes), "recv() must return immutable bytes so a handler can't mutate a shared buffer"


async def test_blocked_recv_hangs_forever_when_peer_closes(peer_thread):
    a, b = ThreadTransport.pair()
    await a.connect()
    peer_thread.run(b.connect())
    pending = peer_thread.submit(b.recv())
    await asyncio.sleep(0.05)
    await a.close()
    with pytest.raises(ConnectionError):
        await asyncio.wrap_future(pending)
    # a hung recv keeps App._listen alive after the client is gone, leaking the server thread


async def test_messages_sent_before_close_are_lost(peer_thread):
    a, b = ThreadTransport.pair()
    await a.connect()
    peer_thread.run(b.connect())
    for message in (b"1", b"2", b"3"):
        await a.send(message)
    await a.close()

    async def drain():
        return [await b.recv() for _ in range(3)]

    assert peer_thread.run(drain()) == [b"1", b"2", b"3"], "close must not discard replies already sent — the last response of a session vanishes"
    with pytest.raises(ConnectionError):
        peer_thread.run(b.recv())


async def test_send_after_peer_closed_reports_success(peer_thread):
    a, b = ThreadTransport.pair()
    await a.connect()
    peer_thread.run(b.connect())
    peer_thread.run(b.close())
    with pytest.raises(ConnectionError):
        await a.send(b"ping")
    # a successful send into a dead inbox makes the caller wait out the whole call timeout


async def test_send_after_peer_thread_died_without_close_reports_success():
    a, b = ThreadTransport.pair()
    await a.connect()
    worker = LoopThread()
    worker.run(b.connect())
    worker.stop()  # peer's loop is gone and close() never ran — a crashed worker thread
    with pytest.raises(ConnectionError):
        await a.send(b"ping")


async def test_send_on_own_closed_endpoint_still_delivers(peer_thread):
    a, b = ThreadTransport.pair()
    await a.connect()
    peer_thread.run(b.connect())
    await a.close()
    with pytest.raises(ConnectionError):
        await a.send(b"ping")


async def test_second_connect_wipes_queued_messages(peer_thread):
    a, b = ThreadTransport.pair()
    await a.connect()
    peer_thread.run(b.connect())
    await a.send(b"queued")
    with pytest.raises(RuntimeError):
        peer_thread.run(b.connect())
    assert peer_thread.run(b.recv()) == b"queued", "a repeated connect silently replaced the inbox and dropped an in-flight request"


async def test_connect_after_close_reopens_endpoint():
    a, _b = ThreadTransport.pair()
    await a.connect()
    await a.close()
    with pytest.raises(RuntimeError):
        await a.connect()
    # other transports refuse reuse after close; a half-reopened endpoint accepts sends that recv() will never return


async def test_both_sides_closing_at_once_raises(peer_thread):
    a, b = ThreadTransport.pair()
    await a.connect()
    peer_thread.run(b.connect())
    other = peer_thread.submit(b.close())
    await a.close()
    await asyncio.wrap_future(other)
    await a.close()
    peer_thread.run(b.close())
    # both shutdown paths race in every App/Client teardown; an exception here aborts cleanup


async def test_cancelled_recv_loses_next_message(peer_thread):
    a, b = ThreadTransport.pair()
    await a.connect()
    peer_thread.run(b.connect())

    async def cancel_one_recv():
        waiter = asyncio.create_task(b.recv())
        await asyncio.sleep(0)
        waiter.cancel()
        await asyncio.gather(waiter, return_exceptions=True)

    peer_thread.run(cancel_one_recv())  # App._await_or_stop cancels recv on every shutdown race
    await a.send(b"after-cancel")
    assert peer_thread.run(b.recv()) == b"after-cancel", "a cancelled recv swallowed the next message — the next RPC never gets a reply"


async def test_fast_sender_grows_peer_inbox_without_bound(peer_thread):
    a, b = ThreadTransport.pair()
    await a.connect()
    peer_thread.run(b.connect())
    with pytest.raises((ConnectionError, OverflowError, asyncio.QueueFull)):
        for _ in range(100_000):
            await a.send(b"x")
    # nobody is reading b; a stalled server thread lets a client queue unbounded memory, while other transports apply queue budgets


async def test_oversized_message_is_accepted(peer_thread):
    a, b = ThreadTransport.pair()
    await a.connect()
    peer_thread.run(b.connect())
    assert hasattr(a, "max_message_size"), "App sizes its response budget from max_message_size; without it, limits are silently skipped"
    with pytest.raises(Exception):
        await a.send(b"x" * (a.max_message_size + 1))


async def test_empty_message_is_delivered(peer_thread):
    a, b = ThreadTransport.pair()
    await a.connect()
    peer_thread.run(b.connect())
    with pytest.raises(Exception):
        await a.send(b"")
    # stdio and TCP reject empty frames; accepting them here makes the same app behave differently per transport


async def test_rpc_call_fails_across_threads(peer_thread):
    client_end, server_end = ThreadTransport.pair()
    app = App(server_end)

    @app.method("add")
    async def add(params: dict, ctx) -> int:
        return params["a"] + params["b"]

    @app.method("boom")
    async def boom(ctx):
        raise ValueError("SECRET")

    server = peer_thread.submit(app._run())
    for _ in range(100):
        if server_end._loop is not None:
            break
        await asyncio.sleep(0.01)

    client = Client(client_end, call_timeout=2.0)
    await client.connect()
    try:
        assert await client.call("add", int, {"a": 2, "b": 3}) == 5, "a JSON-RPC call over threads must return the handler's result"
        with pytest.raises(WireRpcError) as error:
            await client.call("boom", int)
        assert error.value.data is None, "handler exceptions must not leak details across the thread boundary"
    finally:
        await client.close()
    await asyncio.wait_for(asyncio.wrap_future(server), 2.0)
    # if this times out, the server thread never noticed the client left and stays alive forever


async def test_send_before_peer_connects_is_lost(peer_thread):
    a, b = ThreadTransport.pair(connect_timeout=2.0)
    await a.connect()
    sending = asyncio.create_task(a.send(b"early"))
    await asyncio.sleep(0.05)
    assert not sending.done(), "send gave up before the peer had a chance to start; startup races break the first RPC"
    peer_thread.run(b.connect())
    await asyncio.wait_for(sending, 1.0)
    assert peer_thread.run(b.recv()) == b"early", "a message sent while the peer was starting never arrived"


async def test_waiting_send_freezes_sender_event_loop():
    a, _b = ThreadTransport.pair(connect_timeout=0.5)
    await a.connect()
    ticks = 0

    async def heartbeat():
        nonlocal ticks
        while True:
            ticks += 1
            await asyncio.sleep(0.01)

    beat = asyncio.create_task(heartbeat())
    try:
        with pytest.raises(ConnectionError):
            await a.send(b"x")
    finally:
        beat.cancel()
    assert ticks > 10, "waiting for the peer blocked the whole event loop; every other task on this thread stalls"


async def test_peer_closing_before_connect_leaves_sender_waiting_full_timeout():
    a, b = ThreadTransport.pair(connect_timeout=5.0)
    await a.connect()
    sending = asyncio.create_task(a.send(b"x"))
    await asyncio.sleep(0.05)
    await asyncio.to_thread(asyncio.run, b.close())
    with pytest.raises(ConnectionError):
        await asyncio.wait_for(sending, 1.0)
    # a peer that shut down will never connect; making the sender wait it out delays shutdown by connect_timeout


async def test_sender_closing_while_waiting_still_delivers(peer_thread):
    a, b = ThreadTransport.pair(connect_timeout=2.0)
    await a.connect()
    sending = asyncio.create_task(a.send(b"x"))
    await asyncio.sleep(0.05)
    await a.close()
    peer_thread.run(b.connect())
    with pytest.raises(ConnectionError):
        await asyncio.wait_for(sending, 1.0)
    # a message from a closed endpoint looks like a live request to the server, which then replies into nothing


@pytest.mark.parametrize("bad", [0, -1, float("inf"), float("nan"), "5"])
def test_invalid_connect_timeout_is_accepted(bad):
    with pytest.raises(ValueError):
        ThreadTransport.pair(connect_timeout=bad)
    # zero or negative silently disables waiting; inf or nan waits forever on a peer that never starts


async def test_client_first_call_races_server_startup(peer_thread):
    client_end, server_end = ThreadTransport.pair(connect_timeout=2.0)
    app = App(server_end)

    @app.method("add")
    async def add(params: dict, ctx) -> int:
        return params["a"] + params["b"]

    server = peer_thread.submit(app._run())  # no sleep, no polling: the client must not need to know when the server is up
    client = Client(client_end, call_timeout=3.0)
    await client.connect()
    try:
        assert await client.call("add", int, {"a": 2, "b": 3}) == 5, "the first call failed because the server thread had not connected yet"
    finally:
        await client.close()
    await asyncio.wait_for(asyncio.wrap_future(server), 2.0)
