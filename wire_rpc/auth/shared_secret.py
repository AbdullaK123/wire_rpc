
from wire_rpc.auth._util import identity
import asyncio
import hmac
from typing import Any


class SharedSecretAuth:

    def __init__(
        self,
        secret: str,
        *, principal: str = "shared-secret"
    ):
        self._principal = identity(principal)
        self._secret = secret.encode() if isinstance(secret, str) else secret

    async def authenticate(self, connection) -> None:
        reader, writer = connection
        writer.write(len(self._secret).to_bytes(4, 'big') + self._secret)
        await writer.drain()
        length = int.from_bytes(await reader.readexactly(4), 'big')
        if length != 2 or await reader.readexactly(2) != b'OK':
            raise PermissionError('Authentication rejected')

    async def login(self, request: Any) -> Any:
        return None

    async def logout(self, request: Any) -> Any:
        return None

    async def verify(self, request: Any):

        reader: asyncio.StreamReader = request[0]
        writer: asyncio.StreamWriter = request[1]

        try:
            length_bytes = await asyncio.wait_for(reader.readexactly(4), timeout=5.0)
            length = int.from_bytes(length_bytes, "big")

            if length > 1024:
                writer.close()
                return None

            secret = await asyncio.wait_for(reader.readexactly(length), timeout=5.0)

            if hmac.compare_digest(secret, self._secret):
                ok = b'OK'
                writer.write(len(ok).to_bytes(4, "big"))
                writer.write(ok)
                await writer.drain()

                return self._principal

            reject = b'REJECT'
            writer.write(len(reject).to_bytes(4, "big"))
            writer.write(reject)
            await writer.drain()
            writer.close()
            return None

        except (asyncio.TimeoutError, asyncio.IncompleteReadError, ConnectionError):
            writer.close()
            return None