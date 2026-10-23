"""Browser Origin checks shared by the HTTP and WebSocket servers."""
from collections.abc import Iterable

from aiohttp import web
from yarl import URL

from wire_rpc.logger import logger

_logged: set[str] = set()


class OriginPolicy:
    """Decide whether a browser request's ``Origin`` may reach the server.

    With no allowlist, a browser request is accepted only from the server's own
    origin: the same scheme, host and port the request arrived on. An explicit
    allowlist replaces that rule with exact matching; an empty one rejects every
    browser request. Requests without an ``Origin`` header (non-browser clients)
    are always accepted.
    """

    def __init__(self, allowed_origins: Iterable[str] | None):
        self._allowed = None if allowed_origins is None else frozenset(allowed_origins)

    def check(self, request: web.Request) -> None:
        """Raise ``HTTPForbidden`` (403) if ``request`` comes from an origin that isn't allowed."""
        origin = request.headers.get('Origin')
        if not isinstance(origin, str) or self._allows(origin, request):
            return
        if origin not in _logged and len(_logged) < 64:
            # Logged once per origin, so a misconfiguration is visible without letting a
            # flood of hostile origins flood the log.
            _logged.add(origin)
            logger.warning('Rejected browser request from origin %r; add it to allowed_origins if it is yours',
                           origin)
        raise web.HTTPForbidden(text='Origin not allowed')

    def _allows(self, origin: str, request: web.Request) -> bool:
        if self._allowed is not None:
            return origin in self._allowed
        return _same_origin(origin, request)


def _same_origin(origin: str, request: web.Request) -> bool:
    """Return True if ``origin`` names the scheme, host and port ``request`` arrived on."""
    try:
        claimed = URL(origin)
        own = URL(f"{'https' if request.secure else 'http'}://{request.host}")
    except (ValueError, TypeError):
        return False
    if claimed.scheme not in ('http', 'https') or claimed.host is None or own.host is None:
        return False  # includes the opaque origin "null"
    return (claimed.scheme, claimed.host.lower(), claimed.port) == (own.scheme, own.host.lower(), own.port)
