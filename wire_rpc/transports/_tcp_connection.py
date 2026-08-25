"""Per-connection TCP state."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field


@dataclass(slots=True)
class TcpConnection:
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    write_lock: asyncio.Lock = field(default_factory=asyncio.Lock)


__all__ = ["TcpConnection"]
