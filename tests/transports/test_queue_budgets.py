import asyncio
import pytest
from wire_rpc.transports._queue import PayloadQueue


async def test_byte_budget_blocks_payload_even_when_message_slots_are_free():
    queue = PayloadQueue(100,max_bytes=4,per_peer=10)
    await queue.put(('a',b'1234'))
    entered = asyncio.Event()
    async def put():
        entered.set()
        await queue.put(('b',b'x'))
    task = asyncio.create_task(put()); await entered.wait()
    assert not task.done(), 'message count limits alone must not allow queued bytes to exceed the memory budget'
    assert queue.stats['bytes'] == 4, 'blocked puts must not reserve additional payload bytes inside the queue'
    await queue.get(); await task
    queue.close()


async def test_noisy_peer_cannot_occupy_other_peers_admission_slots():
    queue = PayloadQueue(100,max_bytes=100,per_peer=1)
    await queue.put(('a',b'a'))
    entered = asyncio.Event()
    async def put():
        entered.set(); await queue.put(('a',b'b'))
    task = asyncio.create_task(put()); await entered.wait()
    await queue.put(('b',b'other'))
    assert queue.stats['peers'] == 2, 'one peer at its individual quota must not prevent other peers from entering the queue'
    queue.drop_peer('a'); await task
    queue.close()


async def test_close_wakes_blocked_producers_and_consumers():
    full = PayloadQueue(1,max_bytes=1,per_peer=1); empty = PayloadQueue()
    await full.put(b'x')
    put = asyncio.create_task(full.put(b'y')); get = asyncio.create_task(empty.get())
    full.close(); empty.close()
    results = await asyncio.gather(put,get,return_exceptions=True)
    assert all(isinstance(result,ConnectionError) for result in results), 'shutdown must wake queue waiters instead of leaking tasks indefinitely'
    assert full.stats['bytes'] == 0, 'shutdown must release retained payloads'
