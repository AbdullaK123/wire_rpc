
import asyncio
import hmac
from typing import Any


class SharedSecretAuth:

    def __init__(
        self,
        secret: str
    ):
        self._secret = secret.encode() if isinstance(secret, str) else secret

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

                addr = writer.get_extra_info("peername")
                return f"{addr[0]}:{addr[1]}"

            reject = b'REJECT'
            writer.write(len(reject).to_bytes(4, "big"))
            writer.write(reject)
            await writer.drain()
            writer.close()
            return None

        except (asyncio.TimeoutError, asyncio.IncompleteReadError, ConnectionError):
            writer.close()
            return None