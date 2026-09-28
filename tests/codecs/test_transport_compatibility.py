import pytest
from wire_rpc import App, Client, MulticastApp
from wire_rpc.codecs.msgspec import MsgSpecMsgPackCodec
from wire_rpc.transports.http import HttpServerTransport,HttpClientTransport
from wire_rpc.transports.websocket import WsServerTransport,MulticastWsServerTransport,WsClientTransport


@pytest.mark.parametrize('factory,transport',[
    (App,HttpServerTransport('127.0.0.1',0)),
    (App,WsServerTransport()),
    (MulticastApp,MulticastWsServerTransport()),
    (Client,HttpClientTransport('http://localhost/rpc')),
    (Client,WsClientTransport()),
])
def test_binary_codec_on_text_transport_fails_before_startup(factory,transport):
    with pytest.raises(ValueError,match='is_text'):
        factory(transport,codec=MsgSpecMsgPackCodec())
