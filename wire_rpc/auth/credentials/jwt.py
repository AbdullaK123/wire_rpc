"""Offline JWT verification against explicitly trusted keys; no token-directed URLs."""
from collections.abc import Mapping
from typing import Any
from wire_rpc.auth._util import identity, token_text
from wire_rpc.auth.errors import AuthUnavailableError


class JwtTokenValidator:
    def __init__(self, keys: Mapping[str, Any], *, algorithm: str, issuer: str, audience: str, leeway: float = 0):
        import jwt
        if algorithm not in {'RS256', 'RS384', 'RS512', 'ES256', 'ES384', 'ES512', 'EdDSA', 'HS256', 'HS384', 'HS512'}:
            raise ValueError('Unsupported JWT algorithm')
        if not keys or not issuer or not audience or not 0 <= leeway <= 60:
            raise ValueError('JWT needs trusted keys, issuer, audience and bounded leeway')
        if algorithm.startswith('HS') and any(not isinstance(k, (str, bytes)) or len(k) < 32 for k in keys.values()):
            raise ValueError('HMAC JWT keys must contain at least 32 bytes of high-entropy material')
        self._jwt, self._keys = jwt, dict(keys)
        self._algorithm, self._issuer, self._audience, self._leeway = algorithm, issuer, audience, leeway

    async def validate(self, token: str) -> str | None:
        if not token_text(token, 16384):
            return None
        try:
            header = self._jwt.get_unverified_header(token)
            kid = header.get('kid')
            if not isinstance(kid, str) or len(kid) > 128 or kid not in self._keys:
                return None
            if header.get('alg') != self._algorithm or header.get('crit') or header.get('jku') or header.get('x5u'):
                return None
            claims = self._jwt.decode(token, self._keys[kid], algorithms=[self._algorithm],
                issuer=self._issuer, audience=self._audience, leeway=self._leeway,
                options={'require': ['exp', 'iat', 'iss', 'aud', 'sub']})
            # Reject bools and non-finite/coercible timestamp values accepted by some decoders.
            import math
            for field in ('exp', 'iat', 'nbf'):
                if field in claims and (type(claims[field]) not in (int, float) or not math.isfinite(claims[field])):
                    return None
            return identity(claims['sub'])
        except (self._jwt.InvalidTokenError, ValueError, TypeError, OverflowError):
            return None
        except Exception:
            raise AuthUnavailableError('JWT verification unavailable') from None
