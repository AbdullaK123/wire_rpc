"""Map TLS-verified certificate DER fingerprints to stable service identities."""
import hashlib
import ssl
from collections.abc import Mapping
from typing import Any

from wire_rpc.auth._util import identity


class ClientCertificateAuth:
    """Identifies TCP clients by the TLS certificate they presented.

    The server's ``ssl_context`` must use ``CERT_REQUIRED`` so the certificate is
    actually verified; otherwise every client is rejected.

    Args:
        fingerprints: Lowercase SHA-256 hex of each client certificate's DER bytes,
            mapped to the principal it identifies.
    """

    def __init__(self, fingerprints: Mapping[str, str]):
        self._principals = {}
        for fingerprint, principal in fingerprints.items():
            if len(fingerprint) != 64 or any(c not in '0123456789abcdef' for c in fingerprint):
                raise ValueError('Certificate fingerprints must be lowercase SHA-256 hex')
            self._principals[fingerprint] = identity(principal)

    async def verify(self, request: Any) -> str | None:
        """Return the principal for the connection's verified certificate, or ``None``."""
        transport = request[1] if isinstance(request, tuple) else request.transport
        if transport is None:
            return None
        tls = transport.get_extra_info('ssl_object')
        if tls is None or tls.context.verify_mode != ssl.CERT_REQUIRED:
            return None
        certificate = tls.getpeercert(binary_form=True)
        if not certificate:
            return None
        return self._principals.get(hashlib.sha256(certificate).hexdigest())

    async def revalidate(self, connection: Any) -> str | None:
        """Check the certificate again before a request; same as ``verify``."""
        return await self.verify(connection)
