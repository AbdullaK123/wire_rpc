"""Credential adapters with bounded inputs and safe backend failure semantics."""
import asyncio
import hashlib
import hmac
import time
import math
import re
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any

from wire_rpc._validation import positive_limit
from wire_rpc.auth._util import identity, token_text, DependencyLifecycle
from wire_rpc.auth.errors import AuthUnavailableError
from wire_rpc.auth.sessions.protocol import SessionStore


class CallbackCredentialValidator:
    def __init__(self, callback: Callable[[dict[str, Any]], Awaitable[str | None]]):
        self._callback = callback

    async def validate(self, credentials: dict[str, Any]) -> str | None:
        if type(credentials) is not dict:
            return None
        try:
            result = await self._callback(credentials)
            return None if result is None else identity(result)
        except Exception:
            raise AuthUnavailableError('Credential backend unavailable') from None


@dataclass(frozen=True)
class PasswordAccount:
    principal: str
    password_hash: str
    enabled: bool = True


class PasswordCredentialValidator:
    def __init__(self, lookup: Callable[[str], Awaitable[PasswordAccount | None]], *, max_concurrency: int = 4):
        from argon2 import PasswordHasher
        positive_limit('max_concurrency', max_concurrency)
        self._lookup = lookup
        self._hasher = PasswordHasher()
        self._dummy = self._hasher.hash('wire-rpc-dummy-not-an-account')
        self._limit = max_concurrency
        self._active = 0

    async def validate(self, credentials: dict[str, Any]) -> str | None:
        from argon2.exceptions import VerifyMismatchError, VerificationError, InvalidHashError
        if type(credentials) is not dict:
            return None
        username, password = credentials.get('username'), credentials.get('password')
        if not isinstance(username, str) or not 1 <= len(username) <= 255:
            return None
        if not isinstance(password, str) or not 1 <= len(password) <= 1024:
            return None
        try:
            if len(password.encode()) > 4096:
                return None
        except UnicodeEncodeError:
            return None
        if self._active >= self._limit:
            raise AuthUnavailableError('Password verification capacity unavailable')
        self._active += 1
        task: asyncio.Task[bool] | None = None
        deferred_release = False
        try:
            account = await self._lookup(username)
            encoded = account.password_hash if account is not None else self._dummy
            # Shield the worker: cancellation must not release capacity while Argon2 still runs.
            task = asyncio.create_task(asyncio.to_thread(self._hasher.verify, encoded, password))
            try:
                valid = await asyncio.shield(task)
            except VerifyMismatchError:
                return None
            except (VerificationError, InvalidHashError):
                raise AuthUnavailableError('Invalid password record') from None
            return identity(account.principal) if valid and account is not None and account.enabled else None
        except asyncio.CancelledError:
            if task is not None and not task.done():
                def finished(done: asyncio.Task[bool]) -> None:
                    self._active -= 1
                    if not done.cancelled():
                        done.exception()
                task.add_done_callback(finished)
                deferred_release = True
                # The callback owns this admission slot until native work ends.
                raise
            raise
        except AuthUnavailableError:
            raise
        except Exception:
            raise AuthUnavailableError('Credential backend unavailable') from None
        finally:
            # A cancelled worker transferred the slot to its completion callback.
            if not deferred_release:
                self._active -= 1


class StaticPasswordCredentialValidator(PasswordCredentialValidator):
    def __init__(self, accounts: Mapping[str, PasswordAccount], *, max_concurrency: int = 4):
        snapshot = dict(accounts)
        async def lookup(username: str) -> PasswordAccount | None:
            return snapshot.get(username)
        super().__init__(lookup, max_concurrency=max_concurrency)


@dataclass(frozen=True)
class ApiKeyRecord:
    principal: str
    secret_digest: str
    expires_at: float | None = None
    enabled: bool = True

    def __post_init__(self) -> None:
        identity(self.principal)
        if re.fullmatch(r'[0-9a-f]{64}', self.secret_digest) is None:
            raise ValueError('API key digest must be SHA-256 hex')
        if type(self.enabled) is not bool:
            raise ValueError('API key enabled flag must be boolean')
        if self.expires_at is not None and (type(self.expires_at) not in (int, float) or not math.isfinite(self.expires_at)):
            raise ValueError('API key expiry must be finite')

    @classmethod
    def from_secret(cls, principal: str, secret: str, *, expires_at: float | None = None) -> 'ApiKeyRecord':
        identity(principal)
        if not token_text(secret) or len(secret) < 32:
            raise ValueError('API keys require at least 32 characters of high-entropy secret material')
        return cls(principal, hashlib.sha256(secret.encode()).hexdigest(), expires_at)


class ApiKeyValidator:
    """Tokens are key_id.secret. Lookup is live, so revocation applies to the next check."""
    def __init__(self, lookup: Callable[[str], Awaitable[ApiKeyRecord | None]]):
        self._lookup = lookup

    async def validate(self, token: str) -> str | None:
        if not token_text(token) or '.' not in token:
            return None
        key_id, secret = token.split('.', 1)
        if not 1 <= len(key_id) <= 128 or len(secret) < 32:
            return None
        try:
            record = await self._lookup(key_id)
            actual = hashlib.sha256(secret.encode()).hexdigest()
            expected = record.secret_digest if record else '0' * 64
            valid = hmac.compare_digest(actual, expected)
            if not valid or record is None or not record.enabled:
                return None
            if record.expires_at is not None and record.expires_at <= time.time():
                return None
            return identity(record.principal)
        except Exception:
            raise AuthUnavailableError('API key backend unavailable') from None


class SessionTokenValidator(DependencyLifecycle):
    def __init__(self, store: SessionStore):
        super().__init__(store)
        self._store = store

    async def validate(self, token: str) -> str | None:
        return await self._store.validate(token)
