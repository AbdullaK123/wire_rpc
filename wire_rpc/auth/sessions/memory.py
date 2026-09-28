"""Bounded single-process sessions. Each operation has no suspension point."""
import heapq
import json
import time
from typing import Any
from wire_rpc.auth.errors import AuthUnavailableError, SessionCapacityError
from wire_rpc.auth.sessions._base import SessionStoreBase


class InMemorySessionStore(SessionStoreBase):
    def __init__(self, ttl: float = 86400, *, max_sessions: int = 10000):
        super().__init__(ttl, max_sessions=max_sessions)
        self._sessions: dict[str, tuple[str, float]] = {}
        self._expiry: list[tuple[float, str]] = []

    def _prune(self) -> None:
        now = time.monotonic()
        while self._expiry and self._expiry[0][0] <= now:
            expiry, key = heapq.heappop(self._expiry)
            entry = self._sessions.get(key)
            if entry is not None and entry[1] == expiry:
                del self._sessions[key]
        if len(self._expiry) > 2 * self._max_sessions:
            self._expiry = [(expiry, key) for key, (_, expiry) in self._sessions.items()]
            heapq.heapify(self._expiry)

    async def _execute(self, operation: str, key: str = '', value: str = '', new_key: str = '') -> Any:
        self._prune()
        if operation == 'create':
            if key in self._sessions:
                raise AuthUnavailableError('Session token collision')
            if len(self._sessions) >= self._max_sessions:
                raise SessionCapacityError('Session capacity exhausted')
            expiry = time.monotonic() + self._ttl
            self._sessions[key] = (value, expiry)
            heapq.heappush(self._expiry, (expiry, key))
        elif operation == 'get':
            entry = self._sessions.get(key)
            return entry[0] if entry else None
        elif operation == 'destroy':
            self._sessions.pop(key, None)
            self._prune()
        elif operation == 'rotate':
            if key not in self._sessions:
                return False
            if new_key in self._sessions:
                raise AuthUnavailableError('Session token collision')
            # Rotation preserves absolute expiry; it cannot extend stolen sessions forever.
            self._sessions[new_key] = self._sessions.pop(key)
            heapq.heappush(self._expiry, (self._sessions[new_key][1], new_key))
            self._prune()
            return True
        elif operation == 'revoke':
            keys = [k for k, (record, _) in self._sessions.items() if json.loads(record)['principal'] == value]
            for k in keys:
                del self._sessions[k]
            self._prune()
            return len(keys)
        else:
            raise ValueError('Unknown session operation')
