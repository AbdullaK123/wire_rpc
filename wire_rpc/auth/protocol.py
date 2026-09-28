"""Transport verification is independent of optional interactive login routes."""
from typing import Any, Protocol, runtime_checkable


class Authenticator(Protocol):
    async def verify(self, request: Any) -> str | None: ...


@runtime_checkable
class InteractiveAuthenticator(Authenticator, Protocol):
    async def login(self, request: Any) -> Any: ...
    async def logout(self, request: Any) -> Any: ...


class TokenValidator(Protocol):
    async def validate(self, token: str) -> str | None: ...


class ClientAuthenticator(Protocol):
    async def authenticate(self, connection: Any) -> None: ...
