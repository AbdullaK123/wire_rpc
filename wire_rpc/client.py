"""
Wire RPC client.

Connects to a Wire RPC server via any transport and
makes typed RPC calls.
"""

from typing import Self

import uuid
from wire_rpc.codecs.msgspec import MsgSpecJsonCodec
from wire_rpc.codecs.protocol import Codec
from wire_rpc.request import RawWireRequest
from wire_rpc.transports.protocol import Transport


class WireRpcError(Exception):
    """Raised when the server returns a JSON-RPC error."""
    def __init__(self, code: int, message: str, data=None):
        self.code = code
        self.message = message
        self.data = data
        super().__init__(f"[{code}] {message}")


class Client:

    def __init__(
        self,
        transport: Transport,
        codec: Codec = MsgSpecJsonCodec()
    ):
        self._transport = transport
        self._codec = codec

    async def connect(self):
        await self._transport.connect()

    async def close(self):
        await self._transport.close()

    async def __aenter__(self) -> Self:
        await self._transport.connect()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object
    ):
        await self._transport.close()

    async def call(self, method: str, response_type: type, params: object | None = None) -> object:
        request_id = str(uuid.uuid4())
        request = RawWireRequest(
            method=method,
            id=request_id,
            params=self._codec.convert(params, dict) if params is not None else None
        )
        data = self._codec.encode(request)
        await self._transport.send(data)
        response_bytes = await self._transport.recv()

        # Decode the envelope as a raw dict
        envelope = self._codec.decode(response_bytes, dict)

        # Check for error
        error = envelope.get("error")
        if error is not None:
            raise WireRpcError(
                code=error.get("code", -32603),
                message=error.get("message", "Unknown error"),
                data=error.get("data")
            )

        # Decode the result into the typed target
        result = envelope.get("result")
        if result is None:
            raise WireRpcError(code=-32603, message="Response missing result")

        return self._codec.convert(result, response_type)


    __all__ = [
        "Client",
        "WireRpcError"
    ]