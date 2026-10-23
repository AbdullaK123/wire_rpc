"""Authentication for wire_rpc transports.

Server transports take an ``auth=`` authenticator that turns each connection or
request into a principal (a short identity string) or rejects it. Handlers read
that principal from ``current_request().principal``. Client transports take
credentials (``credential_headers``) or a ``ClientAuthenticator``.

Pick by transport:
    HTTP and WebSocket: ``BearerAuth``, ``ApiKeyAuth``, ``CookieSessionAuth``.
    TCP: ``TcpTokenAuth``, ``HmacChallengeAuth``, ``ClientCertificateAuth``.
"""

from .certificate import ClientCertificateAuth
from .client import credential_headers
from .cookie import CookieSessionAuth
from .errors import AuthUnavailableError, SessionCapacityError
from .http import ApiKeyAuth, BearerAuth
from .protocol import (
           Authenticator,
           ClientAuthenticator,
           InteractiveAuthenticator,
           TokenValidator,
)
from .tcp import (
           HmacChallengeAuth,
           HmacChallengeClientAuth,
           TcpTokenAuth,
           TcpTokenClientAuth,
)

__all__ = ['Authenticator', 'InteractiveAuthenticator', 'TokenValidator', 'ClientAuthenticator',
           'CookieSessionAuth', 'BearerAuth', 'ApiKeyAuth', 'ClientCertificateAuth', 'TcpTokenAuth',
           'TcpTokenClientAuth', 'HmacChallengeAuth', 'HmacChallengeClientAuth', 'credential_headers',
           'AuthUnavailableError', 'SessionCapacityError']
