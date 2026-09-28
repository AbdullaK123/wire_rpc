from dataclasses import dataclass
from typing import Protocol, Any


@dataclass(frozen=True)
class SessionRecord:
    principal: str
    payload: dict[str, Any]


class SessionStore(Protocol):
    async def create(self, user_id: str, payload: dict[str, Any] | None = None) -> str: ...
    async def validate(self, session_id: str) -> str | None: ...
    async def destroy(self, session_id: str) -> None: ...


class ManagedSessionStore(SessionStore, Protocol):
    async def get(self, session_id: str) -> SessionRecord | None: ...
    async def rotate(self, session_id: str) -> str | None: ...
    async def revoke_all(self, user_id: str) -> int: ...
