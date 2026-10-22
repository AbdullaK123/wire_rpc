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
    """Wraps your own async function as a credential validator.

    The callback gets the login body as a dict and returns a principal or ``None``.
    Any exception it raises becomes ``AuthUnavailableError``.
    """

    def __init__(self, callback: Callable[[dict[str, Any]], Awaitable[str | None]]):
        self._callback = callback

    async def validate(self, credentials: dict[str, Any]) -> str | None:
        """Call the callback and check that any principal it returns is valid."""
        if type(credentials) is not dict:
            return None
        try:
            result = await self._callback(credentials)
            return None if result is None else identity(result)
        except Exception:
            raise AuthUnavailableError('Credential backend unavailable') from None


@dataclass(frozen=True)
class PasswordAccount:
    """A stored account for password login.

    Attributes:
        principal: The identity a successful login produces.
        password_hash: An Argon2 hash of the password.
        enabled: Disabled accounts always fail to log in.
    """
    principal: str
    password_hash: str
    enabled: bool = True


class PasswordCredentialValidator:
    """Checks ``{"username": ..., "password": ...}`` against Argon2 password hashes.

    Unknown usernames are checked against a dummy hash so they take as long as real
    ones. Hashing runs in a worker thread, at most ``max_concurrency`` at a time;
    beyond that, logins fail fast with ``AuthUnavailableError``. Needs the
    ``password`` extra.

    Args:
        lookup: Fetches the account for a username, or ``None``.
        max_concurrency: Most password checks running at once.
    """

    def __init__(self, lookup: Callable[[str], Awaitable[PasswordAccount | None]], *, max_concurrency: int = 4):
        from argon2 import PasswordHasher
        positive_limit('max_concurrency', max_concurrency)
        self._lookup = lookup
        self._hasher = PasswordHasher()
        self._dummy = self._hasher.hash('wire-rpc-dummy-not-an-account')
        self._limit = max_concurrency
        self._active = 0

    async def validate(self, credentials: dict[str, Any]) -> str | None:
        """Return the account's principal if the password matches, else ``None``.

        Raises:
            AuthUnavailableError: At capacity, the lookup failed, or a stored hash is invalid.
        """
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
    """A ``PasswordCredentialValidator`` over a fixed username-to-account mapping."""

    def __init__(self, accounts: Mapping[str, PasswordAccount], *, max_concurrency: int = 4):
        snapshot = dict(accounts)
        async def lookup(username: str) -> PasswordAccount | None:
            return snapshot.get(username)
        super().__init__(lookup, max_concurrency=max_concurrency)


@dataclass(frozen=True)
class ApiKeyRecord:
    """A stored API key. Only the SHA-256 digest of the secret is kept.

    Attributes:
        principal: The identity the key grants.
        secret_digest: SHA-256 hex of the secret part of the key.
        expires_at: Unix time after which the key stops working, or ``None``.
        enabled: Disabled keys always fail.
    """
    principal: str
    secret_digest: str
    expires_at: float | None = None
    enabled: bool = True

    def __post_init__(self) -> None:
        """Reject invalid fields."""
        identity(self.principal)
        if re.fullmatch(r'[0-9a-f]{64}', self.secret_digest) is None:
            raise ValueError('API key digest must be SHA-256 hex')
        if type(self.enabled) is not bool:
            raise ValueError('API key enabled flag must be boolean')
        if self.expires_at is not None and (type(self.expires_at) not in (int, float) or not math.isfinite(self.expires_at)):
            raise ValueError('API key expiry must be finite')

    @classmethod
    def from_secret(cls, principal: str, secret: str, *, expires_at: float | None = None) -> 'ApiKeyRecord':
        """Build a record from a plaintext secret, storing only its digest.

        Raises:
            ValueError: The secret is under 32 characters or not printable ASCII.
        """
        identity(principal)
        if not token_text(secret) or len(secret) < 32:
            raise ValueError('API keys require at least 32 characters of high-entropy secret material')
        return cls(principal, hashlib.sha256(secret.encode()).hexdigest(), expires_at)


class ApiKeyValidator:
    """Tokens are key_id.secret. Lookup is live, so revocation applies to the next check."""

    def __init__(self, lookup: Callable[[str], Awaitable[ApiKeyRecord | None]]):
        """Create the validator.

        Args:
            lookup: Fetches the record for a key id, or ``None``.
        """
        self._lookup = lookup

    async def validate(self, token: str) -> str | None:
        """Return the principal for a ``key_id.secret`` token, or ``None``.

        Raises:
            AuthUnavailableError: The lookup failed.
        """
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
    """Uses session ids from a ``SessionStore`` as bearer tokens."""

    def __init__(self, store: SessionStore):
        super().__init__(store)
        self._store = store

    async def validate(self, token: str) -> str | None:
        """Return the session's principal, or ``None`` if it is unknown or expired."""
        return await self._store.validate(token)
