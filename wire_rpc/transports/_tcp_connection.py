"""Per-connection TCP state."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
@dataclass(slots=True)
class TcpConnection:
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    write_lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    created_at: float = field(default_factory=time.monotonic)
    last_activity_at: float = field(default_factory=time.monotonic)

    def touch(self) -> None:
        self.last_activity_at = time.monotonic()

    @property
    def idle_for(self) -> float:
        return time.monotonic() - self.last_activity_at

__all__ = ["TcpConnection"]
