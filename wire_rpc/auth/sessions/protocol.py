from typing import Protocol, Any


class SessionStore(Protocol):

    async def create(self, user_id: str, payload: dict[str, Any] | None = None) -> str:
        ...

    async def validate(self, session_id: str) -> str | None:
        ...

    async def destroy(self, session_id: str):
        ...