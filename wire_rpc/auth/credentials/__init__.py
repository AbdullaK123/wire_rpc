"""Validators that turn credentials into a principal.

``CredentialValidator``s check login bodies (``CookieSessionAuth``); token
validators check bearer tokens and API keys (``BearerAuth``, ``ApiKeyAuth``,
``TcpTokenAuth``). All return ``None`` to reject and raise
``AuthUnavailableError`` when they cannot decide.
"""

from .jwt import JwtTokenValidator
from .protocol import CredentialValidator
from .validators import (
                         ApiKeyRecord,
                         ApiKeyValidator,
                         CallbackCredentialValidator,
                         PasswordAccount,
                         PasswordCredentialValidator,
                         SessionTokenValidator,
                         StaticPasswordCredentialValidator,
)

__all__ = ['CredentialValidator', 'CallbackCredentialValidator', 'PasswordCredentialValidator',
           'StaticPasswordCredentialValidator', 'PasswordAccount', 'ApiKeyRecord',
           'ApiKeyValidator', 'SessionTokenValidator', 'JwtTokenValidator']
