"""The interface for checking login credentials."""

from typing import Protocol, Any

class CredentialValidator(Protocol):
 
    """Checks a login request's credentials, e.g. a username and password."""
    async def validate(self, credentials: dict[str, Any]) -> str | None:
        """Return the principal for ``credentials``, or ``None`` to reject.

        Raises:
            AuthUnavailableError: The decision could not be made.
        """
