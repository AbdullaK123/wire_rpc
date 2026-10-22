"""Shared session logic; each backend implements one atomic ``_execute``."""

import hashlib
import json
import re
import secrets
from typing import Any

from wire_rpc._validation import positive_limit, positive_timeout
from wire_rpc.auth._util import identity
from wire_rpc.auth.errors import AuthUnavailableError
from wire_rpc.auth.sessions.protocol import SessionRecord


class SessionStoreBase:
    """Backends execute one atomic operation; raw tokens never reach storage."""

    def __init__(self, ttl: float = 86400, *, max_sessions: int = 10000):
        """Set the limits shared by every backend.

        Args:
            ttl: Session lifetime in seconds, measured from creation.
            max_sessions: Most live sessions; creating more raises ``SessionCapacityError``.
        """
        positive_timeout('ttl', ttl)
        positive_limit('max_sessions', max_sessions)
        self._ttl, self._max_sessions = ttl, max_sessions

    @staticmethod
    def _digest(token: str) -> str:
        """Hash a session id; only the hash is stored."""
        return hashlib.sha256(token.encode('ascii')).hexdigest()

    @staticmethod
    def _valid(token: object) -> bool:
        """Return True if ``token`` has the shape of an id this store issues."""
        return isinstance(token, str) and re.fullmatch(r'[A-Za-z0-9_-]{43}', token) is not None

    async def _execute(self, operation: str, key: str = '', value: str = '', new_key: str = '') -> Any:
        """Run one operation (create, get, destroy, rotate or revoke) atomically. Backends implement this."""
        raise NotImplementedError

    async def _call(self, operation: str, key: str = '', value: str = '', new_key: str = '') -> Any:
        """Run ``_execute``, turning unexpected backend errors into ``AuthUnavailableError``."""
        try:
            return await self._execute(operation, key, value, new_key)
        except AuthUnavailableError:
            raise
        except Exception:
            raise AuthUnavailableError('Session backend unavailable') from None

    async def create(self, user_id: str, payload: dict[str, Any] | None = None) -> str:
        """Start a session for ``user_id`` and return its new random id.

        Args:
            user_id: The principal the session authenticates.
            payload: Extra JSON-serializable data to keep with the session, up to 16 KiB total.

        Raises:
            ValueError: The principal or payload is invalid or too large.
            SessionCapacityError: ``max_sessions`` live sessions already exist.
            AuthUnavailableError: The backend failed.
        """
        identity(user_id)
        if payload is not None and type(payload) is not dict:
            raise ValueError('Session payload must be a JSON object')
        value = json.dumps({'principal': user_id, 'payload': payload or {}}, allow_nan=False, separators=(',', ':'))
        if len(value.encode()) > 16384:
            raise ValueError('Session metadata exceeds 16 KiB')
        token = secrets.token_urlsafe(32)
        await self._call('create', self._digest(token), value)
        return token

    async def get(self, session_id: str) -> SessionRecord | None:
        """Return the session's record, or ``None`` if it is unknown or expired.

        Raises:
            AuthUnavailableError: The backend failed or holds a corrupt record.
        """
        if not self._valid(session_id):
            return None
        value = await self._call('get', self._digest(session_id))
        if value is None:
            return None
        try:
            data = json.loads(value)
            principal = identity(data['principal'])
            payload = data['payload']
            if type(payload) is not dict:
                raise ValueError
            return SessionRecord(principal, payload)
        except (ValueError, KeyError, TypeError):
            raise AuthUnavailableError('Invalid session record') from None

    async def validate(self, session_id: str) -> str | None:
        """Return the session's principal, or ``None`` if it is unknown or expired."""
        record = await self.get(session_id)
        return record.principal if record else None

    async def destroy(self, session_id: str) -> None:
        """End the session; unknown ids are ignored."""
        if self._valid(session_id):
            await self._call('destroy', self._digest(session_id))

    async def rotate(self, session_id: str) -> str | None:
        """Replace the session's id with a new one, keeping its original expiry.

        Call this after a privilege change so an old, possibly leaked id stops working.
        Returns the new id, or ``None`` if the session does not exist.
        """
        if not self._valid(session_id):
            return None
        token = secrets.token_urlsafe(32)
        rotated = await self._call('rotate', self._digest(session_id), new_key=self._digest(token))
        return token if rotated else None

    async def revoke_all(self, user_id: str) -> int:
        """End every session belonging to ``user_id`` and return how many were ended."""
        return int(await self._call('revoke', value=identity(user_id)))
