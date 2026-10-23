"""Per-connection TCP state."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field


async def wait_writer_closed(writer: asyncio.StreamWriter) -> None:
    """Wait for ``writer`` to finish closing without letting cancellation leak to other waiters.

    ``StreamWriter.wait_closed()`` awaits one future shared by every caller, and
    cancelling any caller cancels that future, so every other caller would get a
    ``CancelledError`` it never asked for. Each caller waits on its own task instead.
    """
    waiter = asyncio.ensure_future(writer.wait_closed())
    # Retrieve the outcome even if every caller gave up, so it is never reported as unhandled.
    waiter.add_done_callback(lambda done: done.cancelled() or done.exception())
    await asyncio.shield(waiter)


@dataclass(slots=True)
class TcpConnection:
    """One TCP connection: its streams, authenticated identity, locks and activity clock.

    The locks keep concurrent reads, and concurrent writes, from interleaving frames.
    """
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    principal: str | None = None
    close_timeout: float = 5.0
    read_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    write_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    created_at: float = field(default_factory=time.monotonic)
    last_activity_at: float = field(default_factory=time.monotonic)

    def touch(self) -> None:
        """Record traffic now, resetting the idle clock."""
        self.last_activity_at = time.monotonic()

    @property
    def idle_for(self) -> float:
        """Seconds since the last recorded traffic."""
        return time.monotonic() - self.last_activity_at

    async def close(self) -> None:
        """Close the stream, aborting it if a graceful close fails or times out."""
        self.writer.close()
        try:
            async with asyncio.timeout(self.close_timeout):
                await wait_writer_closed(self.writer)
        except asyncio.CancelledError:
            transport = getattr(self.writer, 'transport', None)
            if transport is not None:
                transport.abort()
            raise
        except (ConnectionError, OSError, TimeoutError):
            # A reset during cleanup must not mask the original frame error.
            transport = getattr(self.writer, "transport", None)
            if transport is not None:
                transport.abort()


__all__ = ["TcpConnection", "wait_writer_closed"]
