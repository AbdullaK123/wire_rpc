import hashlib
import ssl
from types import SimpleNamespace
from unittest.mock import Mock
import pytest
from wire_rpc.auth import ClientCertificateAuth


@pytest.mark.parametrize('mode', [ssl.CERT_NONE, ssl.CERT_OPTIONAL])
async def test_certificate_identity_requires_tls_layer_to_require_and_verify_client_certificates(mode):
    certificate = b'untrusted-cert'
    tls = Mock(context=SimpleNamespace(verify_mode=mode), getpeercert=Mock(return_value=certificate))
    writer = Mock(get_extra_info=Mock(return_value=tls))
    auth = ClientCertificateAuth({hashlib.sha256(certificate).hexdigest():'alice'})
    assert await auth.verify((None,writer)) is None, 'a configured certificate fingerprint must not bypass mandatory TLS client-certificate verification'
    assert tls.getpeercert.call_count == 0, 'certificate bytes must not establish identity when TLS verification policy is insufficient'


async def test_proxy_headers_cannot_impersonate_a_verified_client_certificate():
    request = Mock(transport=None, headers={'X-Client-Cert':'trusted'})
    auth = ClientCertificateAuth({hashlib.sha256(b'trusted').hexdigest():'alice'})
    assert await auth.verify(request) is None, 'forwarded certificate headers are attacker-controlled unless a separate trusted-proxy boundary is implemented'


async def test_unknown_certificate_does_not_inherit_another_clients_identity():
    tls = Mock(context=SimpleNamespace(verify_mode=ssl.CERT_REQUIRED), getpeercert=Mock(return_value=b'other'))
    auth = ClientCertificateAuth({hashlib.sha256(b'trusted').hexdigest():'alice'})
    assert await auth.verify((None, Mock(get_extra_info=Mock(return_value=tls)))) is None, 'successful TLS verification alone must not grant an identity absent from the application certificate mapping'
