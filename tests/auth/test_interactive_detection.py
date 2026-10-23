import pytest

from wire_rpc.auth import HmacChallengeAuth
from wire_rpc.auth.protocol import InteractiveAuthenticator
from wire_rpc.auth.shared_secret import SharedSecretAuth


@pytest.mark.parametrize('factory', [
    lambda: SharedSecretAuth('a-long-test-only-secret-with-32-chars'),
    lambda: HmacChallengeAuth({'key': ('service', b's' * 32)}),
])
def test_tcp_only_authenticator_exposes_login_routes(factory):
    auth = factory()
    assert not isinstance(auth, InteractiveAuthenticator), (
        'HTTP/WebSocket servers mount POST /login and /logout for interactive authenticators; '
        'no-op methods here create public routes that return nothing and crash the handler'
    )
