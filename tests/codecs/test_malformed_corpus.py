from tests.helpers import UnusedTransport, response_bytes
import random
import pytest
from wire_rpc import App
from wire_rpc._execution import process
from wire_rpc.codecs.msgspec import MsgSpecJsonCodec, MsgSpecMsgPackCodec
from wire_rpc.codecs.pydantic import PydanticCodec


@pytest.mark.parametrize('factory',[MsgSpecJsonCodec,MsgSpecMsgPackCodec,PydanticCodec])
async def test_seeded_garbage_and_truncations_cannot_escape_public_request_errors(factory):
    codec=factory(); app=App(UnusedTransport(),codec=codec); calls=[]
    @app.method('mutate')
    async def mutate(ctx):
        calls.append('mutation')
    valid=codec.encode({'jsonrpc':'2.0','method':'mutate','id':'a'})
    rng=random.Random(8127)
    corpus=[valid[:index] for index in range(len(valid))]
    corpus += [rng.randbytes(rng.randrange(128)) for _ in range(256)]
    for payload in corpus:
        result=codec.decode(await process(app,payload,app._dispatch),dict)
        assert result['error']['code'] in (-32700,-32600), 'hostile bytes must become normalized errors without escaping the per-request failure boundary'
    assert calls == [], 'malformed or truncated envelopes must never reach a mutating handler'
