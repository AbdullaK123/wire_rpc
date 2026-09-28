"""Per-connection TCP state."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field


@dataclass(slots=True)
class TcpConnection:
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    principal: str | None = None
    close_timeout: float = 5.0
    read_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    write_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    created_at: float = field(default_factory=time.monotonic)
    last_activity_at: float = field(default_factory=time.monotonic)

    def touch(self) -> None:
        self.last_activity_at = time.monotonic()

    @property
    def idle_for(self) -> float:
        return time.monotonic() - self.last_activity_at

    async def close(self) -> None:
        self.writer.close()
        try:
            async with asyncio.timeout(self.close_timeout):
                await self.writer.wait_closed()
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


__all__ = ["TcpConnection"]
