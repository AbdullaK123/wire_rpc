"""HTTP/upgrade authenticators. Authentication methods never fall back to each other."""
from aiohttp import web
from wire_rpc.auth._util import DependencyLifecycle, token_text, validated
from wire_rpc.auth.protocol import TokenValidator


class BearerAuth(DependencyLifecycle):
    def __init__(self, validator: TokenValidator, *, require_tls: bool = True):
        super().__init__(validator)
        self._validator, self._require_tls = validator, require_tls

    async def verify(self, request: web.Request) -> str | None:
        if self._require_tls and not request.secure:
            return None
        values = request.headers.getall('Authorization', [])
        if len(values) != 1:
            return None
        scheme, separator, token = values[0].partition(' ')
        if scheme.lower() != 'bearer' or not separator or not token_text(token, 16384):
            return None
        return await validated(self._validator, token)


class ApiKeyAuth(DependencyLifecycle):
    def __init__(self, validator: TokenValidator, *, header: str = 'X-API-Key', require_tls: bool = True):
        import re
        if re.fullmatch(r'[A-Za-z0-9-]{1,64}', header) is None:
            raise ValueError('Invalid API key header')
        super().__init__(validator)
        self._validator, self._header, self._require_tls = validator, header, require_tls

    async def verify(self, request: web.Request) -> str | None:
        if self._require_tls and not request.secure:
            return None
        values = request.headers.getall(self._header, [])
        if len(values) != 1 or not token_text(values[0]):
            return None
        return await validated(self._validator, values[0])
