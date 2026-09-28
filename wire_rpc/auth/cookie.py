from wire_rpc.auth.credentials.protocol import CredentialValidator
from wire_rpc.auth.sessions.memory import InMemorySessionStore
from wire_rpc.auth.sessions.protocol import SessionStore
from aiohttp import web

from wire_rpc.auth._util import DependencyLifecycle, validated, token_text
from wire_rpc.auth.errors import AuthUnavailableError

class CookieSessionAuth(DependencyLifecycle):
    """
    Cookie-based session authenticator for web transports.
 
    Plugs into WsServerTransport or MulticastWsTransport.
    Adds a POST /login route and validates cookies on
    WebSocket upgrade.
    """
 
    def __init__(
        self,
        credential_validator: CredentialValidator,
        session_store: SessionStore | None = None,
        cookie_name: str = "session",
        secure: bool = True,
        max_age: int = 86400,
        *, require_tls: bool | None = None,
    ):
        from wire_rpc._validation import positive_limit
        positive_limit("max_age", max_age)
        self._validator = credential_validator
        self._sessions = session_store if session_store is not None else InMemorySessionStore(ttl=max_age)
        from http.cookies import SimpleCookie
        probe: SimpleCookie = SimpleCookie()
        probe[cookie_name] = "check"
        self._require_tls = secure if require_tls is None else require_tls
        self._cookie_name = cookie_name
        self._secure = secure
        self._max_age = max_age
        super().__init__(self._validator, self._sessions)

    async def login(self, request: web.Request) -> web.Response:
        if self._require_tls and not request.secure:
            return web.json_response({"ok": False, "error": "TLS required"}, status=403)
        try:
            body = await request.json()
        except Exception:
            return web.json_response({"ok": False, "error": "Invalid request body"}, status=400)
 
        if type(body) is not dict:
            return web.json_response({"ok": False, "error": "Invalid request body"}, status=400)
        try:
            user_id = await validated(self._validator, body)
        except AuthUnavailableError:
            return web.json_response({"ok": False, "error": "Authentication unavailable"}, status=503)
        if user_id is None:
            return web.json_response({"ok": False, "error": "Invalid credentials"}, status=401)
 
        try:
            session_id = await self._sessions.create(user_id)
        except AuthUnavailableError:
            return web.json_response({'ok':False,'error':'Session capacity unavailable'},status=503)
 
        response = web.json_response({"ok": True, "user_id": user_id})
        response.set_cookie(
            self._cookie_name,
            session_id,
            httponly=True,
            samesite="Strict",
            secure=self._secure,
            max_age=self._max_age,
        )
        return response
 
    def _session_id(self, request: web.Request) -> str | None:
        matches = 0
        for header in request.headers.getall('Cookie', []):
            for part in header.split(';'):
                name, separator, _ = part.strip().partition('=')
                if separator and name == self._cookie_name:
                    matches += 1
        if matches > 1:
            return None
        return request.cookies.get(self._cookie_name)

    async def verify(self, request: web.Request) -> str | None:
        if self._require_tls and not request.secure:
            return None
        session_id = self._session_id(request)
        if session_id is None or not token_text(session_id):
            return None
        return await validated(self._sessions, session_id)
 
    async def logout(self, request: web.Request) -> web.Response:
        """Optional — destroy the session and clear the cookie."""
        if self._require_tls and not request.secure:
            return web.json_response({"ok": False, "error": "TLS required"}, status=403)
        session_id = self._session_id(request)
        if session_id:
            try:
                await self._sessions.destroy(session_id)
            except AuthUnavailableError:
                return web.json_response({"ok": False, "error": "Authentication unavailable"}, status=503)
        response = web.json_response({"ok": True})
        response.del_cookie(self._cookie_name)
        return response