"""Interfaces for session stores."""

from dataclasses import dataclass
from typing import Any, Protocol


@dataclass(frozen=True)
class SessionRecord:
    """A stored session.

    Attributes:
        principal: The identity the session authenticates.
        payload: Extra data saved with the session.
    """
    principal: str
    payload: dict[str, Any]


class SessionStore(Protocol):
    """The operations cookie login needs: create, check and end sessions."""

    async def create(self, user_id: str, payload: dict[str, Any] | None = None) -> str:
        """Start a session for ``user_id`` and return its id."""

    async def validate(self, session_id: str) -> str | None:
        """Return the session's principal, or ``None`` if unknown or expired."""

    async def destroy(self, session_id: str) -> None:
        """End the session."""


class ManagedSessionStore(SessionStore, Protocol):
    """A session store that also supports reading, rotating and mass revocation."""

    async def get(self, session_id: str) -> SessionRecord | None:
        """Return the full session record, or ``None``."""

    async def rotate(self, session_id: str) -> str | None:
        """Give the session a new id and return it, or ``None`` if it does not exist."""

    async def revoke_all(self, user_id: str) -> int:
        """End every session for ``user_id`` and return how many were ended."""
