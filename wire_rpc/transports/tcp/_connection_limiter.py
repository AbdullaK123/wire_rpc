"""Connection admission limiting for connection-oriented transports."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager


class ConnectionLimitExceeded(Exception):
    """Raised when a transport has no connection capacity remaining."""


class ConnectionLimiter:
    """Reserve and release connection capacity with structured cleanup."""

    def __init__(self, limit: int):
        if limit <= 0:
            raise ValueError("Connection limit must be greater than zero")

        self._limit = limit
        self._active = 0

    @asynccontextmanager
    async def slot(self) -> AsyncIterator[None]:
        # There is intentionally no await between the capacity check and the
        # reservation. On the asyncio event loop this makes admission atomic
        # with respect to other tasks using this limiter.
        if self._active >= self._limit:
            raise ConnectionLimitExceeded

        self._active += 1
        try:
            yield
        finally:
            self._active -= 1


__all__ = ["ConnectionLimiter", "ConnectionLimitExceeded"]
