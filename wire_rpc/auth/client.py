"""Client HTTP credentials; application owns login flows and token refresh."""
from collections.abc import Mapping
from wire_rpc.auth._util import token_text


def credential_headers(*, bearer: str | None = None, api_key: str | None = None,
                       api_key_header: str = 'X-API-Key') -> Mapping[str, str]:
    if (bearer is None) == (api_key is None):
        raise ValueError('Choose exactly one credential scheme')
    import re
    if re.fullmatch(r'[A-Za-z0-9-]{1,64}', api_key_header) is None:
        raise ValueError('Invalid header name')
    token = bearer if bearer is not None else api_key
    if not token_text(token, 16384):
        raise ValueError('Invalid credential')
    return {'Authorization': f'Bearer {bearer}'} if bearer is not None else {api_key_header: str(api_key)}
