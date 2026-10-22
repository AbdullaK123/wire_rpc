"""JSON-RPC between two threads of the same process."""

from wire_rpc._validation import positive_limit
from wire_rpc.transports.errors import InvalidFrameSizeError
import asyncio
from typing import Self


class ThreadTransport:
    """One end of an in-process channel between two threads, each with its own event loop.

    Create both ends with ``pair()`` and give one to each thread. Each end must call
    ``connect()`` from the event loop it will be used on. Messages are copied on
    send, so the sender may reuse its buffer.

    Args:
        max_message_size: Largest message in bytes; empty messages are rejected too.
        max_queue_size: Most unread messages an end will hold. Sending to a full
            end raises instead of waiting.

    Sending fails with ``ConnectionError`` until the other end has connected, so
    the caller must wait for the other thread to be ready before its first send.

    Usage:
        client_end, server_end = ThreadTransport.pair()
        threading.Thread(target=App(server_end).run).start()
        # ... wait until server_end is connected ...
        async with Client(client_end) as client:
            total = await client.call("add", int, {"a": 2, "b": 3})
    """

    def __init__(
        self,
        max_message_size: int = 16*1024*1024,
        max_queue_size: int = 256
    ):
        positive_limit("max_message_size", max_message_size)
        positive_limit("max_queue_size", max_queue_size)
        self._peer: ThreadTransport | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._inbox: asyncio.Queue[bytes | None] | None = None
        self._closed = False
        self._max_message_size = max_message_size
        self._max_queued = max_queue_size

    @property
    def max_message_size(self) -> int:
        """Largest message in bytes this end will send."""
        return self._max_message_size


    @classmethod
    def pair(
        cls,
        max_message_size: int = 16*1024*1024,
        max_queue_size: int = 256
    ) -> tuple["ThreadTransport", "ThreadTransport"]:
        """Create two connected ends that share the same limits."""
        a, b = cls(max_message_size=max_message_size, max_queue_size=max_queue_size), cls(max_message_size=max_message_size, max_queue_size=max_queue_size)
        a._peer, b._peer = b, a
        return a, b

    async def connect(self):
        """Bind this end to the running event loop and start accepting messages.

        Raises:
            RuntimeError: This end is already connected or has been closed.
        """
        if self._loop is not None or self._closed:
            raise RuntimeError("Transport already connected or closed")
        self._inbox = asyncio.Queue()
        self._loop = asyncio.get_running_loop()

    async def send(self, data: bytes):
        """Deliver one message to the other end's inbox without waiting.

        Raises:
            InvalidFrameSizeError: The message is empty or larger than ``max_message_size``.
            ConnectionError: Either end is closed or not yet connected, the other end's
                event loop has stopped, or its inbox is full.
        """
        peer = self._peer
        inbox = peer._inbox if peer is not None else None
        if self._closed or peer is None or peer._closed or peer._loop is None or inbox is None:
            raise ConnectionError("Thread channel closed")
        if len(data) == 0 or len(data) > self._max_message_size:
            raise InvalidFrameSizeError(self._max_message_size) 
        try:
            data = bytes(data)
            if inbox.qsize() >= peer._max_queued:
                raise ConnectionError("Peer inbox full")
            peer._loop.call_soon_threadsafe(inbox.put_nowait, data)
        except RuntimeError as exc:
            raise ConnectionError("Thread channel closed") from exc

    async def recv(self) -> bytes:
        """Wait for the next message from the other end.

        Raises:
            ConnectionError: Either end has closed. Messages that arrived before the
                other end closed are still returned first.
        """
        if self._closed or self._inbox is None:
            raise ConnectionError("Thread channel closed")
        data = await self._inbox.get()
        if data is None:
            raise ConnectionError("Thread channel closed")
        return data

    async def close(self):
        """Close this end and wake any pending ``recv`` on both ends. Safe to call twice."""
        if self._closed:
            return
        self._closed = True
        if self._inbox is not None:
            self._inbox.put_nowait(None)
        peer = self._peer
        if peer is not None and peer._loop is not None and peer._inbox is not None:
            try:
                peer._loop.call_soon_threadsafe(peer._inbox.put_nowait, None)
            except RuntimeError:
                pass

    async def __aenter__(self) -> Self:
        """Connect and return this end."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Close this end."""
        await self.close()


__all__ = [
    "ThreadTransport"
]
