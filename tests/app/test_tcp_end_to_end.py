import asyncio
import pytest
from wire_rpc import Client, MulticastApp, current_request
from wire_rpc.auth.shared_secret import SharedSecretAuth
from wire_rpc.auth.hmac_challenge import HmacChallengAuth
from wire_rpc.codecs.msgspec import MsgSpecJsonCodec, MsgSpecMsgPackCodec
from wire_rpc.codecs.pydantic import PydanticCodec
from wire_rpc.transports.tcp import TcpClientTransport, TcpMulticastServerTransport


@pytest.mark.parametrize('auth_factory',[SharedSecretAuth,HmacChallengAuth])
@pytest.mark.parametrize('codec_factory',[MsgSpecJsonCodec,MsgSpecMsgPackCodec,PydanticCodec])
async def test_authenticated_peer_survives_handler_and_encoding_failures_without_exposing_secrets(auth_factory,codec_factory):
    ready = asyncio.Event()
    class Server(TcpMulticastServerTransport):
        async def connect(self):
            await super().connect(); ready.set()
    auth = auth_factory('a-long-test-only-secret')
    transport = Server(host='127.0.0.1',port=0,keep_alive=None,auth=auth,shutdown_timeout=0.1)
    app = MulticastApp(transport,codec=codec_factory())
    @app.method('fail')
    async def fail(ctx):
        raise ValueError('VERY_SECRET')
    @app.method('unserializable')
    async def unserializable(ctx):
        return object()
    @app.method('identity')
    async def identity(ctx):
        return current_request().principal
    task = asyncio.create_task(app._run()); await ready.wait()
    port = transport._server.sockets[0].getsockname()[1]
    client = Client(TcpClientTransport(host='127.0.0.1',port=port,keep_alive=None,auth=auth),codec=codec_factory())
    try:
        await client.connect()
        from wire_rpc.client import WireRpcError
        for method in ['fail','unserializable']:
            with pytest.raises(WireRpcError) as error:
                await client.call(method,str)
            assert error.value.code == -32603 and error.value.data is None, 'handler/codec failures must produce sanitized request-local errors over real TCP'
        principal = await client.call('identity',str)
        assert principal.startswith('127.0.0.1:'), 'the authenticated identity must survive transport dispatch without using caller-controlled params'
        assert app.stats['active'] == 0, 'failed RPC work must release execution accounting for subsequent calls'
    finally:
        await client.close(); await app.stop(); await task
    assert not transport._clients and not app._tasks, 'application stop must release peer sockets and owned dispatch tasks'
