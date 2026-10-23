"""Client.notify, Client.batch and server notifications, end to end and against a lying server."""
import asyncio
from typing import Self

import pytest

from tests.e2e.harness import KINDS, MULTICAST, serve
from wire_rpc import MulticastApp
from wire_rpc.client import (
    BatchCall,
    BatchNotification,
    Client,
    Notification,
    WireRpcError,
)

pytestmark = pytest.mark.timeout(30)


@pytest.mark.parametrize('kind', KINDS)
async def test_notify_is_lost_or_answered(kind):
    async with serve(kind) as server:
        async with Client(server.wire(), call_timeout=5) as client:
            await client.notify('note', {'n': 1})
            await client.notify('note', {'n': 2})
            assert await client.call('notes', list) == [1, 2], 'notifications must run, in order, and leave no reply behind for the next call'


@pytest.mark.parametrize('kind', KINDS)
async def test_batch_results_are_misaligned(kind):
    async with serve(kind) as server:
        async with Client(server.wire(), call_timeout=5) as client:
            results = await client.batch([
                BatchCall('add', int, {'a': 1, 'b': 2}),
                BatchNotification('note', {'n': 'in-batch'}),
                BatchCall('fail', int),
                BatchCall('no.such.method', int),
                BatchCall('echo', dict, {'x': [1, 2]}),
            ])
            assert results[0] == 3 and results[1] is None and results[4] == {'x': [1, 2]}, 'results must line up with the items that produced them'
            assert isinstance(results[2], WireRpcError) and results[2].code == -32603, 'a failed call is returned as its error, not raised'
            assert 'SECRET' not in str(results[2])
            assert isinstance(results[3], WireRpcError) and results[3].code == -32601
            assert await client.call('notes', list) == ['in-batch'], 'the notification inside the batch must run, and the client must stay usable'


@pytest.mark.parametrize('kind', KINDS)
async def test_all_notification_batch_waits_for_a_reply(kind):
    async with serve(kind) as server:
        async with Client(server.wire(), call_timeout=5) as client:
            assert await client.batch([BatchNotification('note', {'n': 1}), BatchNotification('note', {'n': 2})]) == [None, None]
            assert await client.call('notes', list) == [1, 2], 'a batch of notifications gets no reply; waiting for one would hang until the timeout'


@pytest.mark.parametrize('kind', ['tcp-multi', 'http', 'thread'])
async def test_refused_batch_kills_the_client(kind):
    async with serve(kind, app_options={'max_batch_size': 2}) as server:
        async with Client(server.wire(), call_timeout=5) as client:
            with pytest.raises(WireRpcError) as refused:
                await client.batch([BatchCall('add', int, {'a': i, 'b': 0}) for i in range(3)])
            assert refused.value.code == -32600
            assert await client.call('add', int, {'a': 1, 'b': 1}) == 2, 'a batch the server refused is a normal reply; the channel is still good'


async def test_empty_batch_is_sent():
    async with serve('thread') as server:
        async with Client(server.wire(), call_timeout=5) as client:
            with pytest.raises(ValueError):
                await client.batch([])
            assert await client.call('add', int, {'a': 1, 'b': 1}) == 2


@pytest.mark.parametrize('kind', sorted(MULTICAST))
@pytest.mark.parametrize('style', ['sync', 'async'])
async def test_broadcast_during_call_is_dropped(kind, style):
    gate = asyncio.Event()
    seen: list[Notification] = []

    def sync_handler(notification: Notification) -> None:
        seen.append(notification)

    async def async_handler(notification: Notification) -> None:
        seen.append(notification)

    def setup(app):
        @app.method('slow')
        async def slow(ctx) -> str:
            await gate.wait()
            return 'done'

    async with serve(kind, setup=setup) as server:
        handler = sync_handler if style == 'sync' else async_handler
        async with Client(server.wire(), call_timeout=5, on_notification=handler) as client:
            assert await client.call('add', int, {'a': 0, 'b': 0}) == 0
            call = asyncio.create_task(client.call('slow', str))
            await asyncio.sleep(0.1)
            assert isinstance(server.app, MulticastApp)
            await server.app.broadcast('tick', {'n': 1})
            await server.app.broadcast('tick', {'n': 2})
            await asyncio.sleep(0.1)
            gate.set()
            assert await call == 'done'
        assert seen == [Notification('tick', {'n': 1}), Notification('tick', {'n': 2})], 'broadcasts arriving mid-call must reach on_notification, in order'


@pytest.mark.parametrize('kind', sorted(MULTICAST))
async def test_failing_notification_handler_fails_the_call(kind):
    gate = asyncio.Event()

    def explode(notification: Notification) -> None:
        raise RuntimeError('handler bug')

    def setup(app):
        @app.method('slow')
        async def slow(ctx) -> str:
            await gate.wait()
            return 'done'

    async with serve(kind, setup=setup) as server:
        async with Client(server.wire(), call_timeout=5, on_notification=explode) as client:
            assert await client.call('add', int, {'a': 0, 'b': 0}) == 0
            call = asyncio.create_task(client.call('slow', str))
            await asyncio.sleep(0.1)
            assert isinstance(server.app, MulticastApp)
            await server.app.broadcast('tick')
            await asyncio.sleep(0.1)
            gate.set()
            assert await call == 'done', 'a bug in the application callback must not fail an unrelated call'
            assert await client.call('add', int, {'a': 1, 'b': 1}) == 2


@pytest.mark.parametrize('kind', sorted(MULTICAST))
async def test_idle_listener_misses_broadcasts(kind):
    async with serve(kind) as server:
        async with Client(server.wire(), call_timeout=5) as listener:
            assert await listener.call('add', int, {'a': 0, 'b': 0}) == 0  # registered with the server
            waiting = asyncio.create_task(listener.receive_notification())
            await asyncio.sleep(0.05)
            assert isinstance(server.app, MulticastApp)
            await server.app.broadcast('tick', {'n': 7})
            assert await asyncio.wait_for(waiting, 5) == Notification('tick', {'n': 7})


@pytest.mark.parametrize('kind', sorted(MULTICAST))
async def test_malformed_server_notification_is_trusted(kind):
    gate = asyncio.Event()
    seen: list[Notification] = []

    def setup(app):
        @app.method('slow')
        async def slow(ctx) -> str:
            await gate.wait()
            return 'done'

    async with serve(kind, setup=setup) as server:
        async with Client(server.wire(), call_timeout=5, on_notification=seen.append) as client:
            assert await client.call('add', int, {'a': 0, 'b': 0}) == 0
            call = asyncio.create_task(client.call('slow', str))
            await asyncio.sleep(0.1)
            await server.transport.broadcast(server.codec.encode({'jsonrpc': '2.0', 'method': 5}))
            gate.set()
            with pytest.raises(ConnectionError):
                await call
        assert seen == [], 'a notification with a non-string method is a broken server, not something to hand the application'


async def test_receive_notification_breaks_http_client():
    async with serve('http') as server:
        async with Client(server.wire(), call_timeout=5) as client:
            with pytest.raises(RuntimeError):
                await client.receive_notification()
            assert await client.call('add', int, {'a': 1, 'b': 1}) == 2, 'asking for push over HTTP is a usage error and must not close the client'


class ScriptedTransport:
    """Replies with whatever the test scripts, after rewriting ids from the last request."""

    def __init__(self, codec, reply):
        self._codec, self._reply, self._ids = codec, reply, []
        self.closed = False

    async def connect(self):
        pass

    async def close(self):
        self.closed = True

    async def send(self, data: bytes):
        sent = self._codec.decode(data, object)
        self._ids = [item.get('id') for item in sent] if isinstance(sent, list) else [sent.get('id')]

    async def recv(self) -> bytes:
        return self._codec.encode(self._reply(self._ids))

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, exc_type: type[BaseException] | None, exc_val: BaseException | None,
                        exc_tb: object) -> None:
        await self.close()


def ok(request_id):
    return {'jsonrpc': '2.0', 'result': 1, 'id': request_id}


@pytest.mark.parametrize('reply', [
    pytest.param(lambda ids: [ok(ids[0])], id='missing-id'),
    pytest.param(lambda ids: [ok(ids[0]), ok(ids[0])], id='duplicate-id'),
    pytest.param(lambda ids: [ok(ids[0]), ok('someone-else')], id='foreign-id'),
    pytest.param(lambda ids: [ok(ids[0]), ok(ids[1]), ok('extra')], id='extra-entry'),
    pytest.param(lambda ids: ok(ids[0]), id='single-object'),
    pytest.param(lambda ids: [ok(ids[0]), {'jsonrpc': '2.0', 'id': ids[1]}], id='neither-result-nor-error'),
    pytest.param(lambda ids: [ok(ids[0]), 'junk'], id='non-object-entry'),
    pytest.param(lambda ids: [], id='empty-array'),
])
async def test_mismatched_batch_reply_is_accepted(reply):
    from wire_rpc.codecs.msgspec import MsgSpecJsonCodec
    codec = MsgSpecJsonCodec()
    transport = ScriptedTransport(codec, reply)
    client = Client(transport, codec=codec, call_timeout=5)
    await client.connect()
    with pytest.raises(ConnectionError):
        await client.batch([BatchCall('a', int), BatchCall('b', int)])
    assert transport.closed, 'a reply that does not match the batch means the channel is out of step; it must be closed'
