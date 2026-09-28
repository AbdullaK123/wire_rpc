from tests.helpers import UnusedTransport, response_bytes
import pytest
from wire_rpc import App
from wire_rpc._execution import process
from wire_rpc.codecs.msgspec import MsgSpecJsonCodec, MsgSpecMsgPackCodec
from wire_rpc.codecs.pydantic import PydanticCodec


@pytest.mark.parametrize('codec_factory',[MsgSpecJsonCodec,MsgSpecMsgPackCodec,PydanticCodec])
@pytest.mark.parametrize('envelope',[
    [],[{'jsonrpc':'2.0','method':'mutate','id':1}],
    {'method':'mutate','id':1},
    {'jsonrpc':'1.0','method':'mutate','id':1},
    {'jsonrpc':'2.0','method':'mutate','id':True},
    {'jsonrpc':'2.0','method':'mutate','id':1.5},
    {'jsonrpc':'2.0','method':'x'*256,'id':1},
])
async def test_invalid_or_unsupported_envelopes_do_not_reach_handlers(codec_factory,envelope):
    codec=codec_factory(); app=App(UnusedTransport(),codec=codec); calls=[]
    @app.method('mutate')
    async def mutate(ctx):
        calls.append('write')
    response=codec.decode(await process(app,codec.encode(envelope),app._dispatch),dict)
    assert response['error']['code'] == -32600, 'validly encoded but unsupported envelopes must fail request validation rather than execute ambiguous calls'
    assert not calls, 'invalid protocol metadata must be rejected before application side effects'


async def test_oversized_handler_response_becomes_small_safe_error():
    codec=MsgSpecJsonCodec(); app=App(UnusedTransport(),max_response_size=2048)
    @app.method('huge')
    async def huge(ctx):
        return 'sensitive'*1024
    response=await process(app,codec.encode({'jsonrpc':'2.0','method':'huge','id':1}),app._dispatch)
    response = response_bytes(response)
    assert len(response) <= 2048 and b'sensitive' not in response, 'oversized handler output must not bypass the response byte policy or be copied into errors'
    assert codec.decode(response,dict)['error']['code'] == -32603, 'response size rejection must remain local to that RPC'


async def test_maximum_escaped_request_id_still_fits_safe_error_budget():
    codec=MsgSpecJsonCodec(); app=App(UnusedTransport(),max_response_size=2048)
    @app.method('fail')
    async def fail(ctx):
        raise ValueError('secret')
    request_id='\x00'*255
    result=await process(app,codec.encode({'jsonrpc':'2.0','method':'fail','id':request_id}),app._dispatch)
    result = response_bytes(result)
    assert len(result) <= 2048, 'escaping a maximum-length correlation ID must not make fallback errors exceed the transport budget'
    assert codec.decode(result,dict)['id'] == request_id, 'error size guards must preserve response ownership even for heavily escaped identifiers'
