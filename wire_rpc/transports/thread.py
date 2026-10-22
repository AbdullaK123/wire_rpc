"""JSON-RPC between two threads of the same process."""

import threading
from wire_rpc._validation import positive_limit, positive_timeout
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
        connect_timeout: Seconds a send waits for the other end to connect.

    A send made before the other end has connected waits for it, up to
    ``connect_timeout``, so neither thread has to start first.

    Usage:
        client_end, server_end = ThreadTransport.pair()
        threading.Thread(target=App(server_end).run).start()
        async with Client(client_end) as client:
            total = await client.call("add", int, {"a": 2, "b": 3})
    """

    def __init__(
        self,
        max_message_size: int = 16*1024*1024,
        max_queue_size: int = 256,
        connect_timeout: float = 5.0
    ):
        positive_limit("max_message_size", max_message_size)
        positive_limit("max_queue_size", max_queue_size)
        positive_timeout("connect_timeout", connect_timeout)
        self._connect_timeout = connect_timeout
        self._ready = threading.Event()
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
        max_queue_size: int = 256,
        connect_timeout: float = 5.0
    ) -> tuple["ThreadTransport", "ThreadTransport"]:
        """Create two connected ends that share the same limits and timeout."""
        a, b = cls(max_message_size, max_queue_size, connect_timeout), cls(max_message_size, max_queue_size, connect_timeout)
        a._peer, b._peer = b, a
        return a, b

    async def connect(self):
        """Bind this end to the running event loop and release sends waiting on it.

        Raises:
            RuntimeError: This end is already connected or has been closed.
        """
        if self._loop is not None or self._closed:
            raise RuntimeError("Transport already connected or closed")
        self._inbox = asyncio.Queue()
        self._loop = asyncio.get_running_loop()
        self._ready.set()

    async def send(self, data: bytes):
        """Deliver one message to the other end's inbox.

        Waits up to ``connect_timeout`` if the other end has not connected yet;
        otherwise returns without waiting.

        Raises:
            InvalidFrameSizeError: The message is empty or larger than ``max_message_size``.
            ConnectionError: Either end is closed, the other end did not connect
                within ``connect_timeout``, its event loop has stopped, or its inbox is full.
        """
        peer = self._peer
        if self._closed or peer is None:
            raise ConnectionError("Thread channel closed")
        if len(data) == 0 or len(data) > self._max_message_size:
            raise InvalidFrameSizeError(self._max_message_size) 
        if not peer._ready.is_set():
            if not await asyncio.to_thread(peer._ready.wait, self._connect_timeout):
                raise ConnectionError("Peer did not connect in time")
        inbox = peer._inbox
        if self._closed or peer._closed or peer._loop is None or inbox is None:
            raise ConnectionError("Thread channel closed")
        try:
            # pyrefly: ignore[unnecessary-type-conversion]
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
        """Close this end and wake any pending ``recv`` or waiting ``send``. Safe to call twice."""
        if self._closed:
            return
        self._closed = True
        self._ready.set()
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
