import asyncio
import time

import pytest

from tests.helpers import UnusedTransport, response_bytes
from wire_rpc import App, MulticastApp
from wire_rpc._execution import process
from wire_rpc.codecs.msgspec import MsgSpecJsonCodec, MsgSpecMsgPackCodec
from wire_rpc.codecs.pydantic import PydanticCodec


@pytest.fixture(params=[MsgSpecJsonCodec, MsgSpecMsgPackCodec, PydanticCodec])
def codec(request):
    return request.param()


@pytest.fixture(params=[App, MulticastApp])
def factory(request):
    return request.param


def dispatcher(app):
    return (lambda req: app._dispatch(req, 'peer')) if isinstance(app, MulticastApp) else app._dispatch


async def call(app, codec, envelope):
    reply = await process(app, codec.encode(envelope), dispatcher(app), 'peer' if isinstance(app, MulticastApp) else None)
    return None if reply is None else codec.decode(reply, object)


def req(method, id=None, **params):
    envelope = {'jsonrpc': '2.0', 'method': method}
    if params:
        envelope['params'] = params
    if id is not None:
        envelope['id'] = id
    return envelope


def by_id(responses):
    return {response['id']: response for response in responses}


def counting_app(factory, codec, **options):
    app = factory(UnusedTransport(), codec=codec, **options)
    calls = []

    @app.method('add')
    async def add(params: dict, ctx) -> int:
        calls.append(params)
        return params['a'] + params['b']

    return app, calls


async def test_valid_batch_is_rejected_as_invalid_request(factory, codec):
    app, calls = counting_app(factory, codec)
    reply = await call(app, codec, [req('add', 1, a=1, b=2), req('add', 2, a=3, b=4)])
    assert isinstance(reply, list), 'JSON-RPC 2.0 servers MUST answer a batch with an array; clients that batch get a protocol error'
    assert by_id(reply)[1]['result'] == 3 and by_id(reply)[2]['result'] == 7, 'each call in a batch must return its own result'
    assert len(calls) == 2, 'every valid call in the batch must execute exactly once'


async def test_one_invalid_item_fails_the_whole_batch(factory, codec):
    app, calls = counting_app(factory, codec)
    bad = {'jsonrpc': '2.0', 'params': {'a': 3, 'b': 4}, 'id': 2}
    reply = await call(app, codec, [req('add', 1, a=1, b=2), bad, req('add', 3, a=5, b=6)])
    assert isinstance(reply, list) and len(reply) == 3, 'every request object in a batch must get exactly one response'
    results = by_id(reply)
    assert results[1]['result'] == 3 and results[3]['result'] == 11, 'a malformed neighbour must not cancel valid calls'
    errors = [r for r in reply if 'error' in r]
    assert len(errors) == 1 and errors[0]['error']['code'] == -32600, 'the malformed item must surface as its own Invalid Request error'
    assert len(calls) == 2, 'valid items must still run when another item is invalid'


async def test_non_object_items_are_silently_dropped(factory, codec):
    app, calls = counting_app(factory, codec)
    reply = await call(app, codec, [1, 2, 3])
    assert isinstance(reply, list) and len(reply) == 3, 'the spec requires one Invalid Request per non-object item, not silence'
    assert all(r['error']['code'] == -32600 and r['id'] is None for r in reply), 'items with no detectable id must get id null'
    assert not calls


async def test_nested_batch_is_executed(factory, codec):
    app, calls = counting_app(factory, codec)
    reply = await call(app, codec, [[req('add', 1, a=1, b=2)]])
    assert isinstance(reply, list) and len(reply) == 1 and reply[0]['error']['code'] == -32600, 'batches do not nest; an inner array is an invalid item'
    assert not calls, 'a nested array must not smuggle calls past batch validation'


async def test_empty_batch_returns_empty_array(factory, codec):
    app, _ = counting_app(factory, codec)
    reply = await call(app, codec, [])
    assert isinstance(reply, dict) and reply['error']['code'] == -32600, 'an empty batch must get a single Invalid Request object, not an array'


async def test_notifications_in_batch_get_responses(factory, codec):
    app, calls = counting_app(factory, codec)
    reply = await call(app, codec, [req('add', 1, a=1, b=2), req('add', a=3, b=4)])
    assert isinstance(reply, list) and len(reply) == 1 and reply[0]['id'] == 1, 'notifications must run but must not produce response entries'
    assert len(calls) == 2, 'a notification inside a batch must still execute'


async def test_all_notification_batch_sends_empty_array(factory, codec):
    app, calls = counting_app(factory, codec)
    reply = await call(app, codec, [req('add', a=1, b=2), req('add', a=3, b=4)])
    assert reply is None, 'a batch of only notifications must produce no reply at all; an empty array desynchronises clients'
    assert len(calls) == 2


async def test_oversized_batch_runs_anyway(factory, codec):
    app, calls = counting_app(factory, codec, max_batch_size=3)
    reply = await call(app, codec, [req('add', i, a=i, b=i) for i in range(4)])
    assert isinstance(reply, dict) and reply['error']['code'] == -32600, 'an over-limit batch must be rejected with one error object'
    assert not calls, 'one small message must not be able to queue unbounded work before the limit is checked'


async def test_batch_at_the_limit_is_rejected(factory, codec):
    app, calls = counting_app(factory, codec, max_batch_size=3)
    reply = await call(app, codec, [req('add', i, a=i, b=i) for i in range(3)])
    assert isinstance(reply, list) and len(reply) == 3, 'max_batch_size is inclusive; an off-by-one rejects legitimate batches'
    assert len(calls) == 3


@pytest.mark.parametrize('bad', [0, -1, True, 1.5, '10'])
def test_invalid_max_batch_size_is_accepted(factory, bad):
    with pytest.raises(ValueError):
        factory(UnusedTransport(), max_batch_size=bad)
    # zero disables batching by accident; a bool or float limit silently becomes 1 or truncates


async def test_handler_timeout_applies_per_item_instead_of_per_batch(factory):
    codec = MsgSpecJsonCodec()
    app = factory(UnusedTransport(), codec=codec, handler_timeout=0.2)
    started = []

    @app.method('hang')
    async def hang(ctx):
        started.append(1)
        await asyncio.Event().wait()

    began = time.monotonic()
    reply = await call(app, codec, [req('hang', i) for i in range(3)])
    elapsed = time.monotonic() - began
    assert elapsed < 0.45, f'batch took {elapsed:.2f}s; a per-item deadline lets one message hold the server for N x handler_timeout'
    assert isinstance(reply, list) and {r['id'] for r in reply} == {0, 1, 2}, 'calls cut off by the batch deadline still need a response each'
    assert all(r['error']['code'] == -32000 for r in reply), 'calls cut off by the batch deadline must report a timeout, not succeed or vanish'


async def test_large_batch_response_exceeds_response_budget(factory):
    codec = MsgSpecJsonCodec()
    app = factory(UnusedTransport(), codec=codec, max_response_size=4096)
    ran = []

    @app.method('blob')
    async def blob(ctx) -> str:
        ran.append(1)
        return 'x' * 1500

    raw = await process(app, codec.encode([req('blob', i) for i in range(5)]), dispatcher(app), 'peer' if isinstance(app, MulticastApp) else None)
    raw = response_bytes(raw)
    assert len(raw) <= 4096, 'a batch reply must fit max_response_size or the transport drops the whole message'
    reply = codec.decode(raw, list)
    assert {r['id'] for r in reply} == {0, 1, 2, 3, 4}, 'every call ran, so every call must be reported; a missing id invites a duplicate retry'
    assert any('result' in r for r in reply), 'responses that fit must be kept rather than failing the whole batch'
    assert all(r['error']['code'] == -32603 for r in reply if 'result' not in r), 'responses that do not fit must become per-call internal errors'
    assert len(ran) == 5


async def test_batch_whose_error_replies_cannot_fit_still_executes(factory):
    codec = MsgSpecJsonCodec()
    app, calls = counting_app(factory, codec, max_response_size=2048)
    long_id = '\x00' * 255  # escapes to about 1.5 KB of JSON per error reply
    reply = await call(app, codec, [req('add', long_id + str(i), a=1, b=1) for i in range(3)])
    assert isinstance(reply, dict) and reply['error']['code'] == -32600, 'a batch that could not even report its own failures must be refused as a whole'
    assert not calls, 'refusing must happen before execution; otherwise calls run with no way to report them'


async def test_batch_leaks_active_request_count(factory, codec):
    app, _ = counting_app(factory, codec)
    await call(app, codec, [req('add', 1, a=1, b=2), {'bad': True}, req('add', a=1, b=1)])
    assert app.stats['active'] == 0, 'a leaked active count makes the server look permanently busy'



async def test_idless_invalid_items_overflow_response_budget(factory):
    codec = MsgSpecJsonCodec()
    app, _ = counting_app(factory, codec, max_response_size=2048)
    batch = [{'jsonrpc': '2.0', 'method': 5}] * 30  # no id, but invalid, so each still earns an error reply
    raw = response_bytes(await process(app, codec.encode(batch), dispatcher(app), 'peer' if isinstance(app, MulticastApp) else None))
    assert len(raw) <= 2048, 'treating id-less invalid items as reply-free notifications lets their errors overflow the transport budget'
