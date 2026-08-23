import time
import uuid
from typing import Any

class InMemorySessionStore:
    """Simple in-memory session store. Good for dev and single-process apps."""
 
    def __init__(self, ttl: int = 86400):
        self._sessions: dict[str, tuple[str, float]] = {}  # session_id -> (user_id, expires_at)
        self._ttl = ttl
 
    async def create(self, user_id: str, payload: dict[str, Any] | None = None) -> str:
        session_id = uuid.uuid4().hex
        self._sessions[session_id] = (user_id, time.time() + self._ttl)
        return session_id
 
    async def validate(self, session_id: str) -> str | None:
        entry = self._sessions.get(session_id)
        if entry is None:
            return None
        user_id, expires_at = entry
        if time.time() > expires_at:
            del self._sessions[session_id]
            return None
        return user_id
 
    async def destroy(self, session_id: str):
        self._sessions.pop(session_id, None)