from typing import Self

import msgspec
import uuid
from wire_rpc.codecs.msgspec import MsgSpecJsonCodec
from wire_rpc.codecs import Codec
from wire_rpc.request import WireRequest
from wire_rpc.transports.protocol import Transport


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
        request = WireRequest(
            method=method,
            id=request_id,
            params=params
        )
        data = self._codec.encode(request)
        await self._transport.send(data)
        response_bytes = await self._transport.recv()
        return self._codec.decode(response_bytes, type=response_type)

