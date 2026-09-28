import pytest

from wire_rpc.transports.http import HttpServerTransport
from wire_rpc.transports.tcp import TcpClientTransport, TcpServerTransport, TcpMulticastServerTransport
from wire_rpc.transports.websocket import WsServerTransport, MulticastWsServerTransport


@pytest.mark.parametrize("factory", [TcpClientTransport, TcpServerTransport, TcpMulticastServerTransport])
@pytest.mark.parametrize("size", [0, -1, 2**32])
def test_tcp_frame_limit_cannot_disable_framing_or_exceed_header_capacity(factory, size):
    with pytest.raises(ValueError):
        factory(max_frame_size=size)


@pytest.mark.parametrize("factory", [TcpMulticastServerTransport, WsServerTransport, MulticastWsServerTransport])
@pytest.mark.parametrize("size", [0, -1])
def test_receive_queue_configuration_cannot_silently_disable_backpressure(factory, size):
    with pytest.raises(ValueError):
        factory(recv_queue_size=size)


@pytest.mark.parametrize("factory", [WsServerTransport, MulticastWsServerTransport])
@pytest.mark.parametrize("size", [0, -1])
def test_websocket_message_size_cannot_disable_payload_limits(factory, size):
    with pytest.raises(ValueError):
        factory(max_msg_size=size)
