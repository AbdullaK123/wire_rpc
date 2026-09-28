"""Bounded, versioned TCP token and identified HMAC handshakes."""
import asyncio
import hashlib
import hmac
import secrets
from weakref import WeakKeyDictionary
from collections.abc import Mapping
from typing import Any
from wire_rpc._validation import positive_timeout
from wire_rpc.auth._util import DependencyLifecycle, identity, token_text, validated
from wire_rpc.auth.protocol import TokenValidator


async def _read(reader: Any, maximum: int) -> bytes:
    length = int.from_bytes(await reader.readexactly(4), 'big')
    if not 0 < length <= maximum:
        raise PermissionError('Invalid authentication frame')
    return await reader.readexactly(length)


async def _write(writer: Any, data: bytes) -> None:
    writer.write(len(data).to_bytes(4, 'big') + data)
    await writer.drain()


def _tls(writer: Any, required: bool) -> None:
    if required and writer.get_extra_info('ssl_object') is None:
        raise PermissionError('Authentication requires TLS')


class TcpTokenAuth(DependencyLifecycle):
    def __init__(self, validator: TokenValidator, *, require_tls: bool = True, timeout: float = 5):
        super().__init__(validator)
        positive_timeout('timeout', timeout)
        self._validator, self._require_tls, self._timeout = validator, require_tls, timeout
        self._tokens: WeakKeyDictionary[Any, str] = WeakKeyDictionary()

    async def verify(self, request: Any) -> str | None:
        reader, writer = request
        try:
            _tls(writer, self._require_tls)
            async with asyncio.timeout(self._timeout):
                data = await _read(reader, 16384)
                if not data.startswith(b'WRPC-TOKEN-1 '):
                    raise PermissionError('Invalid handshake version')
                token = data[13:].decode('ascii')
                if not token_text(token, 16000):
                    raise PermissionError('Invalid token')
                principal = await validated(self._validator, token)
                if principal is None:
                    raise PermissionError('Invalid credentials')
                await _write(writer, b'OK')
                self._tokens[writer] = token
                return principal
        except (PermissionError, UnicodeError, ConnectionError, TimeoutError, asyncio.IncompleteReadError):
            writer.close()
            return None
        except BaseException:
            writer.close()
            raise

    async def revalidate(self, connection: Any) -> str | None:
        token = self._tokens.get(connection[1])
        return await validated(self._validator, token) if token is not None else None


class TcpTokenClientAuth:
    def __init__(self, token: str, *, require_tls: bool = True, timeout: float = 5):
        if not token_text(token, 16000):
            raise ValueError('Invalid token')
        positive_timeout('timeout', timeout)
        self._token, self._require_tls, self._timeout = token, require_tls, timeout

    async def authenticate(self, connection: Any) -> None:
        reader, writer = connection
        try:
            _tls(writer, self._require_tls)
            async with asyncio.timeout(self._timeout):
                await _write(writer, b'WRPC-TOKEN-1 ' + self._token.encode('ascii'))
                if await _read(reader, 2) != b'OK':
                    raise PermissionError('Authentication rejected')
        except BaseException:
            writer.close()
            raise


class HmacChallengeAuth:
    """Version 1 binds a fresh nonce and client ID to the proof. TLS remains required by default."""
    def __init__(self, keys: Mapping[str, tuple[str, bytes]], *, require_tls: bool = True, timeout: float = 5):
        positive_timeout('timeout', timeout)
        self._keys = dict(keys)
        for key_id, (principal, secret) in self._keys.items():
            identity(key_id); identity(principal)
            if not isinstance(secret, bytes) or len(secret) < 32:
                raise ValueError('HMAC keys need at least 32 bytes of high-entropy material')
        self._require_tls, self._timeout = require_tls, timeout
        self._dummy = secrets.token_bytes(32)
        self._verified: WeakKeyDictionary[Any, tuple[str, bytes]] = WeakKeyDictionary()

    async def verify(self, request: Any) -> str | None:
        reader, writer = request
        try:
            _tls(writer, self._require_tls)
            async with asyncio.timeout(self._timeout):
                key_bytes = await _read(reader, 255)
                key_id = key_bytes.decode('utf-8')
                entry = self._keys.get(key_id)
                nonce = secrets.token_bytes(32)
                challenge = b'WRPC-HMAC-1\x00' + nonce
                await _write(writer, challenge)
                proof = await _read(reader, 32)
                expected = hmac.digest(entry[1] if entry else self._dummy, challenge + b'\x00' + key_bytes, 'sha256')
                if not hmac.compare_digest(proof, expected) or entry is None:
                    raise PermissionError('Invalid credentials')
                await _write(writer, b'OK')
                self._verified[writer] = (key_id, hashlib.sha256(entry[1]).digest())
                return entry[0]
        except (PermissionError, UnicodeError, ConnectionError, TimeoutError, asyncio.IncompleteReadError):
            writer.close()
            return None
        except BaseException:
            writer.close()
            raise

    async def revalidate(self, connection: Any) -> str | None:
        verified = self._verified.get(connection[1])
        if verified is None:
            return None
        entry = self._keys.get(verified[0])
        if entry is None or not hmac.compare_digest(hashlib.sha256(entry[1]).digest(), verified[1]):
            return None
        return entry[0]


class HmacChallengeClientAuth:
    def __init__(self, key_id: str, secret: bytes, *, require_tls: bool = True, timeout: float = 5):
        self._key_id = identity(key_id).encode()
        if len(self._key_id) > 255 or not isinstance(secret, bytes) or len(secret) < 32:
            raise ValueError('Invalid HMAC credentials')
        positive_timeout('timeout', timeout)
        self._secret, self._require_tls, self._timeout = secret, require_tls, timeout

    async def authenticate(self, connection: Any) -> None:
        reader, writer = connection
        try:
            _tls(writer, self._require_tls)
            async with asyncio.timeout(self._timeout):
                await _write(writer, self._key_id)
                challenge = await _read(reader, 44)
                if len(challenge) != 44 or not challenge.startswith(b'WRPC-HMAC-1\x00'):
                    raise PermissionError('Invalid challenge')
                await _write(writer, hmac.digest(self._secret, challenge + b'\x00' + self._key_id, 'sha256'))
                if await _read(reader, 2) != b'OK':
                    raise PermissionError('Authentication rejected')
        except BaseException:
            writer.close()
            raise
