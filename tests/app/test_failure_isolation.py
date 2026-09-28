from tests.helpers import UnusedTransport, response_bytes
import asyncio
import pytest
from wire_rpc import App, MulticastApp, current_request
from wire_rpc._execution import process
from wire_rpc.codecs.msgspec import MsgSpecJsonCodec, MsgSpecMsgPackCodec
from wire_rpc.codecs.pydantic import PydanticCodec


@pytest.fixture(params=[MsgSpecJsonCodec, MsgSpecMsgPackCodec, PydanticCodec])
def codec(request):
    return request.param()


@pytest.mark.parametrize('factory', [App, MulticastApp])
async def test_unserializable_result_returns_safe_error_and_next_request_still_runs(factory, codec):
    app = factory(object(), codec=codec)
    @app.method('bad')
    async def bad(ctx):
        return object()
    @app.method('good')
    async def good(ctx):
        return 'still alive'
    async def run(method):
        dispatch = (lambda req: app._dispatch(req, 'peer')) if factory is MulticastApp else app._dispatch
        return codec.decode(await process(app, codec.encode({'jsonrpc':'2.0','method':method,'id':'a'}), dispatch), dict)
    error = await run('bad')
    success = await run('good')
    assert error['error']['code'] == -32603, 'unsupported handler output must fail that request without terminating dispatch'
    assert success['result'] == 'still alive', 'subsequent callers must remain serviceable after serialization failure'


async def test_handler_exception_never_returns_secret_bearing_exception_text(codec):
    app = App(UnusedTransport(), codec=codec)
    @app.method('secret')
    async def secret(ctx):
        raise RuntimeError('postgres://admin:VERY_SECRET@internal/db')
    payload = await process(app, codec.encode({'jsonrpc':'2.0','method':'secret','id':'a'}), app._dispatch)
    payload = response_bytes(payload)
    assert b'VERY_SECRET' not in payload, 'database credentials inside exceptions must never cross the public RPC boundary'
    assert codec.decode(payload, dict)['error']['data'] is None, 'internal diagnostics must not be copied into public error data'


async def test_null_required_params_do_not_reach_handler_side_effects(codec):
    app = App(UnusedTransport(), codec=codec); calls = []
    @app.method('write')
    async def write(params: dict, ctx):
        calls.append(params)
    reply = await process(app, codec.encode({'jsonrpc':'2.0','method':'write','params':None,'id':1}), app._dispatch)
    assert calls == [], 'null parameters must not bypass declared validation and trigger mutations'
    assert codec.decode(reply, dict)['error']['code'] == -32602, 'invalid input must surface as a parameter error rather than a handler crash'


async def test_notification_handler_failure_does_not_emit_an_unowned_reply(codec):
    app = App(UnusedTransport(), codec=codec)
    @app.method('notify')
    async def notify(ctx):
        raise RuntimeError('secret')
    reply = await process(app, codec.encode({'jsonrpc':'2.0','method':'notify'}), app._dispatch)
    assert reply is None, 'a failed notification must not inject a reply into a later request-response exchange'
