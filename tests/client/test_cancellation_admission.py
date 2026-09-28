import asyncio
import pytest
from wire_rpc import Client
from wire_rpc.codecs.msgspec import MsgSpecJsonCodec


async def test_cancelled_lock_waiter_does_not_close_the_active_call():
    codec=MsgSpecJsonCodec(); entered=asyncio.Event(); release=asyncio.Event()
    class Peer:
        closed=False
        async def send(self,data):
            self.request=codec.decode(data,dict); entered.set(); await release.wait()
        async def recv(self):
            return codec.encode({'jsonrpc':'2.0','id':self.request['id'],'result':42})
        async def close(self):
            self.closed=True
    peer=Peer(); client=Client(peer)
    first=asyncio.create_task(client.call('first',int)); await entered.wait()
    second_started=asyncio.Event()
    async def next_call():
        second_started.set(); return await client.call('second',int)
    second=asyncio.create_task(next_call()); await second_started.wait(); second.cancel()
    with pytest.raises(asyncio.CancelledError):
        await second
    assert not peer.closed, 'a caller cancelled before channel ownership must not terminate somebody else\'s in-flight RPC'
    release.set()
    assert await first == 42, 'the active request must remain intact when an admission waiter is cancelled'
