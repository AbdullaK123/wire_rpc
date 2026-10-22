"""Transport verification is independent of optional interactive login routes."""
from typing import Any, Protocol, runtime_checkable


class Authenticator(Protocol):
    """Server-side check that turns a connection or request into a principal."""

    async def verify(self, request: Any) -> str | None:
        """Return the principal, or ``None`` to reject.

        Raises:
            AuthUnavailableError: The decision could not be made, e.g. a backend is down.
        """


@runtime_checkable
class InteractiveAuthenticator(Authenticator, Protocol):
    """An authenticator with login and logout; HTTP servers expose them as ``POST /login`` and ``/logout``."""

    async def login(self, request: Any) -> Any:
        """Handle a login request and return the HTTP response."""

    async def logout(self, request: Any) -> Any:
        """Handle a logout request and return the HTTP response."""


class TokenValidator(Protocol):
    """Maps a bearer token or API key to a principal."""

    async def validate(self, token: str) -> str | None:
        """Return the principal for ``token``, or ``None`` if it is not valid."""


class ClientAuthenticator(Protocol):
    """Client-side half of a connection handshake, e.g. sending a token over TCP."""

    async def authenticate(self, connection: Any) -> None:
        """Prove this client's identity on a freshly opened connection."""
