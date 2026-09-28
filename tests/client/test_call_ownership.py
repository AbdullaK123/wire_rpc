from tests.helpers import UnusedTransport
import asyncio
import pytest
from wire_rpc.client import Client
from wire_rpc.codecs.msgspec import MsgSpecJsonCodec


class Peer(UnusedTransport):
    def __init__(self):
        self.codec = MsgSpecJsonCodec()
        self.sent = []
        self.closed = False
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.release.set()
        self.reply = None
    async def send(self, data):
        self.sent.append(self.codec.decode(data, dict))
        self.entered.set()
        await self.release.wait()
    async def recv(self):
        value = self.reply or {'jsonrpc':'2.0','id':self.sent[-1]['id'],'result':self.sent[-1]['method']}
        return self.codec.encode(value)
    async def close(self):
        self.closed = True


@pytest.mark.parametrize('reply', [
    {'jsonrpc':'2.0','id':'foreign','result':'private'},
    {'jsonrpc':'1.0','id':'foreign','result':'private'},
    {'jsonrpc':'2.0','id':'foreign','result':1,'error':{}},
    {'jsonrpc':'2.0','id':'foreign','error':'not an error object'},
])
async def test_invalid_response_cannot_be_accepted_as_the_current_calls_result(reply):
    peer = Peer(); peer.reply = reply
    client = Client(peer)
    with pytest.raises(ConnectionError):
        await client.call('current', str)
    assert peer.closed, 'a protocol-violating channel must not remain available to misroute subsequent replies'


async def test_concurrent_calls_cannot_interleave_their_send_receive_pairs():
    peer = Peer(); peer.release.clear()
    client = Client(peer)
    first = asyncio.create_task(client.call('first', str))
    await peer.entered.wait()
    entered = asyncio.Event()
    async def second_call():
        entered.set()
        return await client.call('second', str)
    second = asyncio.create_task(second_call())
    await entered.wait()
    try:
        assert len(peer.sent) == 1, 'the next caller must wait for the entire previous exchange, not just its frame write'
    finally:
        peer.release.set()
        results = await asyncio.gather(first, second)
    assert results == ['first','second'], 'concurrent callers must receive results belonging to their own method'


async def test_cancel_after_send_poisoned_channel_cannot_be_reused_by_next_call():
    peer = Peer(); peer.release.clear()
    client = Client(peer)
    call = asyncio.create_task(client.call('mutate', str))
    await peer.entered.wait()
    call.cancel()
    with pytest.raises(asyncio.CancelledError):
        await call
    peer.release.set()
    with pytest.raises(ConnectionError):
        await client.call('next', str)
    assert peer.closed and len(peer.sent) == 1, 'late replies after cancelled mutations must never be consumed by a successor call'
