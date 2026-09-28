"""Bounded single-process session storage; use shared storage across workers."""
import heapq
import time
import uuid
from typing import Any
from wire_rpc._validation import positive_limit, positive_timeout


class SessionCapacityError(Exception):
    pass


class InMemorySessionStore:
    def __init__(self, ttl: int = 86400, *, max_sessions: int = 10000):
        positive_timeout('ttl', ttl)
        positive_limit('max_sessions', max_sessions)
        self._sessions: dict[str, tuple[str, float]] = {}
        self._expiry = []
        self._ttl, self._max_sessions = ttl, max_sessions

    def _prune(self):
        now = time.monotonic()
        while self._expiry and self._expiry[0][0] <= now:
            expiry, key = heapq.heappop(self._expiry)
            entry = self._sessions.get(key)
            if entry is not None and entry[1] == expiry:
                del self._sessions[key]
        if len(self._expiry) > 2 * self._max_sessions:
            self._expiry = [(expiry,key) for key,(_,expiry) in self._sessions.items()]
            heapq.heapify(self._expiry)

    async def create(self, user_id: str, payload: dict[str, Any] | None = None) -> str:
        self._prune()
        if len(self._sessions) >= self._max_sessions:
            raise SessionCapacityError('Session capacity exhausted')
        key = uuid.uuid4().hex
        expiry = time.monotonic() + self._ttl
        self._sessions[key] = (user_id,expiry)
        heapq.heappush(self._expiry,(expiry,key))
        return key

    async def validate(self, session_id: str) -> str | None:
        self._prune()
        entry = self._sessions.get(session_id)
        return entry[0] if entry else None

    async def destroy(self, session_id: str):
        self._sessions.pop(session_id, None)
        self._prune()
