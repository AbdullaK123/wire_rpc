"""Client HTTP credentials; application owns login flows and token refresh."""
from collections.abc import Mapping
from wire_rpc.auth._util import token_text


def credential_headers(*, bearer: str | None = None, api_key: str | None = None,
                       api_key_header: str = 'X-API-Key') -> Mapping[str, str]:
    """Build HTTP headers for exactly one credential scheme.

    Usage:
        HttpClientTransport(url, headers=credential_headers(bearer=token))

    Raises:
        ValueError: Both or neither credential given, or a malformed token or header name.
    """
    if (bearer is None) == (api_key is None):
        raise ValueError('Choose exactly one credential scheme')
    import re
    if re.fullmatch(r'[A-Za-z0-9-]{1,64}', api_key_header) is None:
        raise ValueError('Invalid header name')
    token = bearer if bearer is not None else api_key
    if not token_text(token, 16384):
        raise ValueError('Invalid credential')
    return {'Authorization': f'Bearer {bearer}'} if bearer is not None else {api_key_header: str(api_key)}


def validate_client_credentials(url, headers, cookie_jar, ssl_context, allow_insecure):
    """Refuse client settings that could leak credentials.

    Credentials (headers or cookies) require ``https``/``wss`` and a verifying TLS
    context unless ``allow_insecure`` is set. Credentials in the URL are always refused.

    Raises:
        ValueError: The settings could expose credentials, or a header is malformed.
    """
    import ssl
    from yarl import URL
    parsed = URL(url)
    if parsed.user is not None:
        raise ValueError('URL userinfo is unsupported; supply explicit credential headers')
    if headers or cookie_jar is not None:
        if not allow_insecure and parsed.scheme not in {'https', 'wss'}:
            raise ValueError('Configured headers/cookies require TLS; opt in explicitly for local development')
        if not allow_insecure and ssl_context is not None and (ssl_context.verify_mode != ssl.CERT_REQUIRED or not ssl_context.check_hostname):
            raise ValueError('Credential-bearing clients require server certificate and hostname verification')
    for name, value in (headers or {}).items():
        if not isinstance(name, str) or not isinstance(value, str) or any(ord(c) < 32 or ord(c) == 127 for c in name + value):
            raise ValueError('Invalid client header')
