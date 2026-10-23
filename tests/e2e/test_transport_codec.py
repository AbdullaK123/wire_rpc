"""Layer 1: every transport x codec pairing, no auth, attacked end to end."""
import asyncio
from typing import Any

import aiohttp
import pytest

from tests.e2e.harness import MULTICAST, TRANSPORT_CODECS, serve
from wire_rpc import Client, MulticastApp
from wire_rpc.client import WireRpcError
from wire_rpc.codecs import CodecError
from wire_rpc.transports.errors import TransportError

pytestmark = pytest.mark.timeout(30)

CLEAN_FAILURES = (ConnectionError, WireRpcError, TransportError, CodecError, aiohttp.ClientError, OSError)


def combos(*, kinds=None, exclude=()):
    return [pytest.param(kind, codec, id=f'{kind}-{codec}') for kind, codec in TRANSPORT_CODECS
            if (kinds is None or kind in kinds) and kind not in exclude]


async def exchange(wire, codec, message, *, reply=True) -> Any:
    await wire.send(codec.encode(message))
    if reply:
        return codec.decode(await asyncio.wait_for(wire.recv(), 5), object)
    return None


def req(method, id=None, **params):
    message = {'jsonrpc': '2.0', 'method': method}
    if params:
        message['params'] = params
    if id is not None:
        message['id'] = id
    return message


@pytest.mark.parametrize('kind,codec', combos())
async def test_typed_call_returns_wrong_result(kind, codec):
    async with serve(kind, codec) as server:
        async with Client(server.wire(), codec=server.codec, call_timeout=5) as client:
            assert await client.call('add', int, {'a': 2, 'b': 3}) == 5, 'the most basic RPC must round-trip through this transport and codec'


@pytest.mark.parametrize('kind,codec', combos())
async def test_handler_exception_leaks_secrets_or_kills_channel(kind, codec):
    async with serve(kind, codec) as server:
        async with Client(server.wire(), codec=server.codec, call_timeout=5) as client:
            with pytest.raises(WireRpcError) as error:
                await client.call('fail', int)
            assert error.value.code == -32603, 'a crashing handler must surface as Internal error'
            assert 'SECRET' not in str(error.value) and error.value.data is None, 'exception text must never cross the wire'
            assert await client.call('add', int, {'a': 1, 'b': 1}) == 2, 'one failed call must not poison the channel for the next call'


@pytest.mark.parametrize('kind,codec', combos())
async def test_protocol_errors_are_misreported(kind, codec):
    async with serve(kind, codec) as server:
        async with Client(server.wire(), codec=server.codec, call_timeout=5) as client:
            with pytest.raises(WireRpcError) as missing:
                await client.call('no.such.method', int)
            assert missing.value.code == -32601, 'unknown methods must be Method not found, not a generic failure'
            with pytest.raises(WireRpcError) as invalid:
                await client.call('add', int, {'a': 'two', 'b': 3})
            assert invalid.value.code == -32602, 'params that fail the declared type must be Invalid params'
            with pytest.raises(WireRpcError) as absent:
                await client.call('add', int)
            assert absent.value.code == -32602, 'missing required params must not reach the handler'


@pytest.mark.parametrize('kind,codec', combos())
async def test_awkward_payload_is_altered_in_transit(kind, codec):
    payload = {
        'emoji': '🧪🔥', 'nul': 'a\x00b', 'rtl': 'שלום', 'combining': 'é',
        'quotes': '"\'\\', 'newlines': 'a\r\nb', 'big': 2**63 - 1, 'negative': -2**63,
        'float': 0.1, 'nested': {'deep': [[[[1]]]], 'empty': {}, 'list': []}, 'none': None, 'flag': False,
    }
    async with serve(kind, codec) as server:
        async with Client(server.wire(), codec=server.codec, call_timeout=5) as client:
            assert await client.call('echo', dict, payload) == payload, 'every byte of awkward but legal data must survive the round trip unchanged'


@pytest.mark.parametrize('kind,codec', combos())
async def test_large_payload_under_limit_is_rejected(kind, codec):
    blob = {'data': 'x' * 200_000}
    async with serve(kind, codec) as server:
        async with Client(server.wire(), codec=server.codec, call_timeout=10) as client:
            assert await client.call('echo', dict, blob) == blob, 'a 200 KB message is under every default limit and must work'


@pytest.mark.parametrize('kind,codec', combos())
async def test_concurrent_callers_get_each_others_results(kind, codec):
    async with serve(kind, codec) as server:
        async with Client(server.wire(), codec=server.codec, call_timeout=10) as client:
            results = await asyncio.gather(*(client.call('add', int, {'a': i, 'b': 1000}) for i in range(25)))
            assert results == [i + 1000 for i in range(25)], 'concurrent calls sharing one client must each get their own answer'


@pytest.mark.parametrize('kind,codec', combos())
async def test_notification_produces_stray_reply(kind, codec):
    async with serve(kind, codec) as server:
        wire = server.wire()
        await wire.connect()
        try:
            codec_ = server.codec
            for n in (1, 2, 3):
                await exchange(wire, codec_, req('note', n=n), reply=False)
            reply = await exchange(wire, codec_, req('notes', id='after'))
            assert reply.get('id') == 'after', 'a notification must not produce a reply that the next request then reads as its own'
            assert reply.get('result') == [1, 2, 3], 'notifications must run, in order, before the next request on the same channel'
        finally:
            await wire.close()


@pytest.mark.parametrize('kind,codec', combos())
async def test_mixed_batch_is_mishandled_over_the_wire(kind, codec):
    batch = [req('add', 1, a=1, b=2), {'jsonrpc': '2.0', 'id': 2}, req('note', n='batched'),
             req('nope', 3), req('fail', 4)]
    async with serve(kind, codec) as server:
        wire = server.wire()
        await wire.connect()
        try:
            reply = await exchange(wire, server.codec, batch)
            assert isinstance(reply, list), 'a batch must come back as an array over every transport'
            by_id = {r['id']: r for r in reply}
            assert len(reply) == 4 and set(by_id) == {1, None, 3, 4}, 'every non-notification gets exactly one response'
            assert by_id[1]['result'] == 3
            assert by_id[None]['error']['code'] == -32600
            assert by_id[3]['error']['code'] == -32601
            assert by_id[4]['error']['code'] == -32603 and 'SECRET' not in str(by_id[4])
            after = await exchange(wire, server.codec, req('notes', id='n'))
            assert after['result'] == ['batched'], 'the notification inside the batch must still have run'
        finally:
            await wire.close()


@pytest.mark.parametrize('kind,codec', combos(exclude={'stdio'}))
async def test_oversized_request_hangs_or_crashes_server(kind, codec):
    async with serve(kind, codec, max_message_size=64 * 1024) as server:
        client = Client(server.wire(max_message_size=8 * 1024 * 1024), codec=server.codec, call_timeout=10)
        await client.connect()
        try:
            with pytest.raises(CLEAN_FAILURES):
                await asyncio.wait_for(client.call('echo', dict, {'data': 'x' * 100_000}), 8)
        finally:
            await client.close()
        if kind in MULTICAST:
            async with Client(server.wire(), codec=server.codec, call_timeout=5) as other:
                assert await other.call('add', int, {'a': 1, 'b': 2}) == 3, 'one peer sending an oversized frame must not take the server down for everyone'


@pytest.mark.parametrize('kind,codec', combos(kinds=MULTICAST))
async def test_broadcast_misses_a_connected_peer(kind, codec):
    async with serve(kind, codec) as server:
        first, second = server.wire(), server.wire()
        await first.connect(); await second.connect()
        try:
            # A round trip proves each peer is registered before the broadcast.
            for wire in (first, second):
                await exchange(wire, server.codec, req('add', 'ready', a=0, b=0))
            assert isinstance(server.app, MulticastApp)
            await server.app.broadcast('tick', {'n': 1})
            for wire in (first, second):
                message = server.codec.decode(await asyncio.wait_for(wire.recv(), 5), object)
                assert message == {'jsonrpc': '2.0', 'method': 'tick', 'params': {'n': 1}}, 'every connected peer must receive the broadcast as a notification'
        finally:
            await first.close(); await second.close()


@pytest.mark.parametrize('kind,codec', combos(kinds=MULTICAST))
async def test_broadcast_during_call_corrupts_client(kind, codec):
    gate = asyncio.Event()

    def setup(app):
        @app.method('slow_add')
        async def slow_add(params: dict, ctx) -> int:
            await gate.wait()
            return params['a'] + params['b']

    async with serve(kind, codec, setup=setup) as server:
        async with Client(server.wire(), codec=server.codec, call_timeout=5) as client:
            assert await client.call('add', int, {'a': 0, 'b': 0}) == 0
            call = asyncio.create_task(client.call('slow_add', int, {'a': 20, 'b': 22}))
            await asyncio.sleep(0.1)
            assert isinstance(server.app, MulticastApp)
            await server.app.broadcast('tick', {'n': 1})
            gate.set()
            assert await call == 42, 'a server broadcast arriving mid-call must not be mistaken for the reply or kill the client'


@pytest.mark.parametrize('kind,codec', combos(kinds=MULTICAST))
async def test_peers_reusing_request_ids_get_crossed_replies(kind, codec):
    async with serve(kind, codec) as server:
        wires = [server.wire() for _ in range(4)]
        for wire in wires:
            await wire.connect()
        try:
            replies = await asyncio.gather(*(exchange(wire, server.codec, req('echo', 1, who=index))
                                             for index, wire in enumerate(wires)))
            assert [r['result']['who'] for r in replies] == [0, 1, 2, 3], 'two peers using the same request id must never receive each other\'s replies'
        finally:
            for wire in wires:
                await wire.close()


@pytest.mark.parametrize('kind,codec', combos(kinds=MULTICAST))
async def test_abrupt_disconnect_mid_call_breaks_other_peers(kind, codec):
    gate = asyncio.Event()

    def setup(app):
        @app.method('wait')
        async def wait(ctx) -> str:
            await gate.wait()
            return 'done'

    async with serve(kind, codec, setup=setup) as server:
        doomed = server.wire()
        await doomed.connect()
        await doomed.send(server.codec.encode(req('wait', 1)))
        await asyncio.sleep(0.1)
        await doomed.close()
        gate.set()
        async with Client(server.wire(), codec=server.codec, call_timeout=5) as survivor:
            assert await survivor.call('add', int, {'a': 2, 'b': 2}) == 4, 'a peer vanishing mid-call must not wedge the server for others'
