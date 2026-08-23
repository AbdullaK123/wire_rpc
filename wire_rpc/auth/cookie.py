from wire_rpc.auth.credentials.protocol import CredentialValidator
from wire_rpc.auth.sessions.memory import InMemorySessionStore
from wire_rpc.auth.sessions.protocol import SessionStore
from aiohttp import web

class CookieSessionAuth:
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
    ):
        self._validator = credential_validator
        self._sessions = session_store or InMemorySessionStore(ttl=max_age)
        self._cookie_name = cookie_name
        self._secure = secure
        self._max_age = max_age
 
    async def login(self, request: web.Request) -> web.Response:
        try:
            body = await request.json()
        except Exception:
            return web.json_response({"ok": False, "error": "Invalid request body"}, status=400)
 
        user_id = await self._validator.validate(body)
        if user_id is None:
            return web.json_response({"ok": False, "error": "Invalid credentials"}, status=401)
 
        session_id = await self._sessions.create(user_id)
 
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
 
    async def verify(self, request: web.Request) -> str | None:
        session_id = request.cookies.get(self._cookie_name)
        if session_id is None:
            return None
        return await self._sessions.validate(session_id)
 
    async def logout(self, request: web.Request) -> web.Response:
        """Optional — destroy the session and clear the cookie."""
        session_id = request.cookies.get(self._cookie_name)
        if session_id:
            await self._sessions.destroy(session_id)
        response = web.json_response({"ok": True})
        response.del_cookie(self._cookie_name)
        return response