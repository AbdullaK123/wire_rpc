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
        positive_timeout('ttl', ttl)
        positive_limit('max_sessions', max_sessions)
        self._ttl, self._max_sessions = ttl, max_sessions

    @staticmethod
    def _digest(token: str) -> str:
        return hashlib.sha256(token.encode('ascii')).hexdigest()

    @staticmethod
    def _valid(token: object) -> bool:
        return isinstance(token, str) and re.fullmatch(r'[A-Za-z0-9_-]{43}', token) is not None

    async def _execute(self, operation: str, key: str = '', value: str = '', new_key: str = '') -> Any:
        raise NotImplementedError

    async def _call(self, operation: str, key: str = '', value: str = '', new_key: str = '') -> Any:
        try:
            return await self._execute(operation, key, value, new_key)
        except AuthUnavailableError:
            raise
        except Exception:
            raise AuthUnavailableError('Session backend unavailable') from None

    async def create(self, user_id: str, payload: dict[str, Any] | None = None) -> str:
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
        record = await self.get(session_id)
        return record.principal if record else None

    async def destroy(self, session_id: str) -> None:
        if self._valid(session_id):
            await self._call('destroy', self._digest(session_id))

    async def rotate(self, session_id: str) -> str | None:
        if not self._valid(session_id):
            return None
        token = secrets.token_urlsafe(32)
        rotated = await self._call('rotate', self._digest(session_id), new_key=self._digest(token))
        return token if rotated else None

    async def revoke_all(self, user_id: str) -> int:
        return int(await self._call('revoke', value=identity(user_id)))
