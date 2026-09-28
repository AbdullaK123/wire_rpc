from tests.helpers import UnusedTransport, response_bytes
import asyncio
import pytest
from wire_rpc import App, current_request
from wire_rpc._execution import process
from wire_rpc.codecs.msgspec import MsgSpecJsonCodec


async def test_concurrent_requests_cannot_read_each_others_authenticated_principal():
    codec = MsgSpecJsonCodec(); arrived = asyncio.Event(); release = asyncio.Event(); seen = []
    class Transport(UnusedTransport):
        async def get_principal(self, client_id):
            return {'a':'alice','b':'bob'}[client_id]
    app = App(Transport())
    @app.method('identity')
    async def identity(ctx):
        original = current_request()
        seen.append(original.principal)
        if len(seen) == 2:
            arrived.set()
        await release.wait()
        assert current_request() == original, 'an await must not replace the handler principal with another concurrent caller'
        return original.principal
    data = codec.encode({'jsonrpc':'2.0','method':'identity','id':1})
    tasks = [asyncio.create_task(process(app,data,app._dispatch,key)) for key in ['a','b']]
    await arrived.wait(); release.set()
    results = [codec.decode(response_bytes(value),dict)['result'] for value in await asyncio.gather(*tasks)]
    assert results == ['alice','bob'], 'authorization decisions must use the principal attached to that request'
    with pytest.raises(RuntimeError):
        current_request()


async def test_revoked_identity_is_rejected_before_handler_side_effects():
    codec = MsgSpecJsonCodec(); calls = []
    class Transport(UnusedTransport):
        async def get_principal(self, client_id):
            raise PermissionError('revoked')
    app = App(Transport())
    @app.method('write')
    async def write(ctx):
        calls.append('mutation')
    reply = await process(app,codec.encode({'jsonrpc':'2.0','method':'write','id':1}),app._dispatch)
    assert calls == [], 'revoked sessions must be checked before dispatching mutations'
    assert codec.decode(response_bytes(reply),dict)['error']['code'] == -32001, 'revocation should fail as a controlled access denial'
