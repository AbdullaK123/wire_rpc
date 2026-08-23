from asyncio import Protocol
from typing import Any


class Authenticator(Protocol):
    async def login(self, request: Any) -> Any: 
        ...
    async def verify(self, request: Any) -> str | None: 
        ...