from wire_rpc.transports import websocket as module


async def test_multicast_broadcast_survives_disconnect_while_awaiting_send():
    transport = module.MulticastWsServerTransport()
    delivered = []

    class Socket:
        def __init__(self, name):
            self.name = name

        async def send_str(self, data):
            delivered.append(self.name)
            if self.name == "first":
                transport._clients.pop("first")

    transport._clients = {"first": Socket("first"), "second": Socket("second")}
    await transport.broadcast(b"message")
    assert delivered == ["first", "second"], "one peer disconnecting during broadcast must not abort delivery to remaining peers"
