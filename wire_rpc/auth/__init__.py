"""Authentication for wire_rpc transports.

Server transports take an ``auth=`` authenticator that turns each connection or
request into a principal (a short identity string) or rejects it. Handlers read
that principal from ``current_request().principal``. Client transports take
credentials (``credential_headers``) or a ``ClientAuthenticator``.

Pick by transport:
    HTTP and WebSocket: ``BearerAuth``, ``ApiKeyAuth``, ``CookieSessionAuth``.
    TCP: ``TcpTokenAuth``, ``HmacChallengeAuth``, ``ClientCertificateAuth``.
"""

from .protocol import Authenticator, InteractiveAuthenticator, TokenValidator, ClientAuthenticator
from .cookie import CookieSessionAuth
from .http import BearerAuth, ApiKeyAuth
from .certificate import ClientCertificateAuth
from .tcp import TcpTokenAuth, TcpTokenClientAuth, HmacChallengeAuth, HmacChallengeClientAuth
from .client import credential_headers
from .errors import AuthUnavailableError, SessionCapacityError

__all__ = ['Authenticator', 'InteractiveAuthenticator', 'TokenValidator', 'ClientAuthenticator',
           'CookieSessionAuth', 'BearerAuth', 'ApiKeyAuth', 'ClientCertificateAuth', 'TcpTokenAuth',
           'TcpTokenClientAuth', 'HmacChallengeAuth', 'HmacChallengeClientAuth', 'credential_headers',
           'AuthUnavailableError', 'SessionCapacityError']
