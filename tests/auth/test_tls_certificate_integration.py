import asyncio
import datetime
import hashlib
import ipaddress
import ssl

import aiohttp
import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID, ExtendedKeyUsageOID

from wire_rpc.auth import ClientCertificateAuth
from wire_rpc.transports.http import HttpServerTransport, HttpClientTransport
from tests.helpers import listening_port


@pytest.fixture
def tls_material(tmp_path):
    now = datetime.datetime.now(datetime.UTC)
    ca_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, 'wire-test-ca')])
    ca = (x509.CertificateBuilder().subject_name(ca_name).issuer_name(ca_name)
        .public_key(ca_key.public_key()).serial_number(x509.random_serial_number())
        .not_valid_before(now-datetime.timedelta(minutes=1)).not_valid_after(now+datetime.timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=True,path_length=0),critical=True).sign(ca_key,hashes.SHA256()))
    ca_path = tmp_path/'ca.pem'; ca_path.write_bytes(ca.public_bytes(serialization.Encoding.PEM))
    certificates = {}
    for name, usage in [('server',ExtendedKeyUsageOID.SERVER_AUTH), ('client',ExtendedKeyUsageOID.CLIENT_AUTH)]:
        key = rsa.generate_private_key(public_exponent=65537,key_size=2048)
        builder = (x509.CertificateBuilder().subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME,name)]))
            .issuer_name(ca_name).public_key(key.public_key()).serial_number(x509.random_serial_number())
            .not_valid_before(now-datetime.timedelta(minutes=1)).not_valid_after(now+datetime.timedelta(days=1))
            .add_extension(x509.BasicConstraints(ca=False,path_length=None),critical=True)
            .add_extension(x509.ExtendedKeyUsage([usage]),critical=False))
        if name == 'server':
            builder = builder.add_extension(x509.SubjectAlternativeName([x509.IPAddress(ipaddress.ip_address('127.0.0.1'))]),critical=False)
        cert = builder.sign(ca_key,hashes.SHA256())
        cert_path, key_path = tmp_path/f'{name}.pem', tmp_path/f'{name}.key'
        cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
        key_path.write_bytes(key.private_bytes(serialization.Encoding.PEM,serialization.PrivateFormat.PKCS8,serialization.NoEncryption()))
        certificates[name] = cert
    server = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server.load_cert_chain(tmp_path/'server.pem',tmp_path/'server.key')
    server.load_verify_locations(ca_path); server.verify_mode=ssl.CERT_REQUIRED
    client = ssl.create_default_context(cafile=str(ca_path))
    client.load_cert_chain(tmp_path/'client.pem',tmp_path/'client.key')
    no_certificate = ssl.create_default_context(cafile=str(ca_path))
    fingerprint = hashlib.sha256(certificates['client'].public_bytes(serialization.Encoding.DER)).hexdigest()
    return server, client, no_certificate, fingerprint


async def test_tls_verified_but_unmapped_certificate_cannot_reach_rpc_queue(tls_material):
    server_ssl, client_ssl, _, _ = tls_material
    transport = HttpServerTransport('127.0.0.1',0,auth=ClientCertificateAuth({}),ssl_context=server_ssl)
    await transport.connect()
    client = HttpClientTransport(f'https://127.0.0.1:{listening_port(transport._runner)}/rpc',ssl_context=client_ssl)
    try:
        await client.connect()
        with pytest.raises(ConnectionError):
            await client.send(b'mutation')
        assert not transport._queue and not transport._pending, 'a CA-trusted certificate must still satisfy application identity mapping before any RPC is admitted'
    finally:
        await client.close(); await transport.close()


async def test_missing_client_certificate_cannot_reach_application_authentication(tls_material):
    server_ssl, _, no_certificate, fingerprint = tls_material
    transport = HttpServerTransport('127.0.0.1',0,auth=ClientCertificateAuth({fingerprint:'alice'}),ssl_context=server_ssl)
    await transport.connect()
    client = HttpClientTransport(f'https://127.0.0.1:{listening_port(transport._runner)}/rpc',ssl_context=no_certificate)
    try:
        await client.connect()
        with pytest.raises((aiohttp.ClientError, ConnectionError)):
            await client.send(b'mutation')
        assert not transport._queue, 'TLS client-certificate requirements must be enforced before application parsing or dispatch'
    finally:
        await client.close(); await transport.close()


async def test_certificate_mapping_revocation_is_checked_after_http_admission(tls_material):
    server_ssl, client_ssl, _, fingerprint = tls_material
    auth = ClientCertificateAuth({fingerprint:'alice'})
    transport = HttpServerTransport('127.0.0.1',0,auth=auth,ssl_context=server_ssl)
    await transport.connect()
    client = HttpClientTransport(f'https://127.0.0.1:{listening_port(transport._runner)}/rpc',ssl_context=client_ssl)
    await client.connect()
    request = asyncio.create_task(client.send(b'mutation'))
    try:
        await transport.recv()
        auth._principals.clear()
        with pytest.raises(PermissionError):
            await transport.get_principal()
        assert transport._active is not None, 'the test must revoke a certificate after admission and before application dispatch'
    finally:
        request.cancel(); await asyncio.gather(request,return_exceptions=True)
        await client.close(); await transport.close()
