"""
Wire RPC client.

Connects to a Wire RPC server via any transport and
makes typed RPC calls.
"""

from typing import Self

import msgspec
import uuid
from wire_rpc.codecs.msgspec import MsgSpecJsonCodec
from wire_rpc.codecs.protocol import Codec
from wire_rpc.request import RawWireRequest
from wire_rpc.transports.protocol import Transport


class RawWireResponse(msgspec.Struct):
    """Non-generic response for framework-level decoding."""
    id: str | int | None = None
    result: msgspec.Raw | msgspec.UnsetType = msgspec.UNSET
    error: dict | None = None
    jsonrpc: str = "2.0"


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

    async def call[R: msgspec.Struct](self, method: str, params: msgspec.Struct, response_type: type[R]) -> R:
        request_id = str(uuid.uuid4())
        request = RawWireRequest(
            method=method,
            id=request_id,
            params=msgspec.to_builtins(params)
        )
        data = self._codec.encode(request)
        await self._transport.send(data)
        response_bytes = await self._transport.recv()

        # Decode the envelope
        response = msgspec.json.decode(response_bytes, type=RawWireResponse)

        # Check for error
        if response.error is not None:
            raise WireRpcError(
                code=response.error.get("code", -32603),
                message=response.error.get("message", "Unknown error"),
                data=response.error.get("data")
            )

        # Decode the result into the typed struct
        if response.result is msgspec.UNSET:
            raise WireRpcError(code=-32603, message="Response missing result")

        return msgspec.json.decode(response.result, type=response_type)


    __all__ = [
        "Client",
        "WireRpcError"
    ]