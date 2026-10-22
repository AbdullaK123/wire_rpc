"""Validators that turn credentials into a principal.

``CredentialValidator``s check login bodies (``CookieSessionAuth``); token
validators check bearer tokens and API keys (``BearerAuth``, ``ApiKeyAuth``,
``TcpTokenAuth``). All return ``None`` to reject and raise
``AuthUnavailableError`` when they cannot decide.
"""

from .protocol import CredentialValidator
from .validators import (CallbackCredentialValidator, PasswordCredentialValidator,
                         StaticPasswordCredentialValidator, PasswordAccount,
                         ApiKeyRecord, ApiKeyValidator, SessionTokenValidator)
from .jwt import JwtTokenValidator

__all__ = ['CredentialValidator', 'CallbackCredentialValidator', 'PasswordCredentialValidator',
           'StaticPasswordCredentialValidator', 'PasswordAccount', 'ApiKeyRecord',
           'ApiKeyValidator', 'SessionTokenValidator', 'JwtTokenValidator']
