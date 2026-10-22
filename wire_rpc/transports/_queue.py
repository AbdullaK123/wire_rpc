"""Byte and peer bounded queues with explicit close wakeups."""
import asyncio
from collections import deque, Counter
from wire_rpc._validation import positive_limit


class PayloadQueue[T: bytes | tuple[str, bytes]]:
    """An async queue bounded by message count, total bytes and messages per peer.

    Items are ``bytes`` or ``(peer_id, bytes)``; plain bytes all count against one
    anonymous peer. ``put`` waits for room instead of growing without bound, and
    ``close`` wakes every waiter with ``ConnectionError``.

    Args:
        maxsize: Most messages held at once.
        max_bytes: Most payload bytes held at once.
        per_peer: Most messages held for any single peer.
    """

    def __init__(self, maxsize=256, *, max_bytes=64 * 1024 * 1024, per_peer=16):
        for name, value in [('maxsize',maxsize),('max_bytes',max_bytes),('per_peer',per_peer)]:
            positive_limit(name, value)
        self.maxsize = maxsize
        self.max_bytes = max_bytes
        self.per_peer = per_peer
        self._items: deque[T] = deque()
        self._bytes = 0
        self._counts: Counter[str | None] = Counter()
        self._changed = asyncio.Event()
        self._closed = False

    @property
    def stats(self):
        """Current message count, byte total and number of peers with queued messages."""
        return {'messages':len(self._items), 'bytes':self._bytes, 'peers':len(self._counts)}

    def qsize(self):
        """Number of messages currently queued."""
        return len(self._items)

    @staticmethod
    def _parts(item: bytes | tuple[str, bytes]) -> tuple[str | None, bytes]:
        """Split an item into its peer id (``None`` for plain bytes) and payload."""
        return item if isinstance(item, tuple) else (None, item)

    async def put(self, item: T) -> None:
        """Add an item, waiting while any limit is reached.

        Raises:
            ValueError: The payload alone is larger than ``max_bytes``.
            ConnectionError: The queue is closed.
        """
        peer, data = self._parts(item)
        size = len(data)
        if size > self.max_bytes:
            raise ValueError('One payload exceeds the entire queue budget')
        while True:
            if self._closed:
                raise ConnectionError('Receive queue closed')
            if (len(self._items) < self.maxsize and self._bytes + size <= self.max_bytes
                    and self._counts[peer] < self.per_peer):
                self._items.append(item)
                self._bytes += size
                self._counts[peer] += 1
                self._changed.set()
                return
            self._changed.clear()
            await self._changed.wait()

    async def get(self) -> T:
        """Remove and return the oldest item, waiting if the queue is empty.

        Raises:
            ConnectionError: The queue is closed.
        """
        while True:
            if self._closed:
                raise ConnectionError('Receive queue closed')
            if self._items:
                item = self._items.popleft()
                peer, data = self._parts(item)
                self._bytes -= len(data)
                self._counts[peer] -= 1
                if not self._counts[peer]:
                    del self._counts[peer]
                self._changed.set()
                return item
            self._changed.clear()
            await self._changed.wait()

    def drop_peer(self, peer):
        """Discard every queued message from ``peer``, e.g. after it disconnects."""
        remaining: deque[T] = deque()
        for item in self._items:
            item_peer, data = self._parts(item)
            if item_peer == peer:
                self._bytes -= len(data)
            else:
                remaining.append(item)
        self._items = remaining
        self._counts.pop(peer, None)
        self._changed.set()

    def close(self):
        """Discard everything and make all current and future ``put``/``get`` raise."""
        self._closed = True
        self._items.clear()
        self._counts.clear()
        self._bytes = 0
        self._changed.set()
