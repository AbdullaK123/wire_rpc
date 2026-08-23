from typing import Any, Protocol


class Authenticator(Protocol):
    async def login(self, request: Any) -> Any: 
        ...
    async def verify(self, request: Any) -> str | None: 
        ...
    async def logout(self, request: Any) -> Any:
        ...