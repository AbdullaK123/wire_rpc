from typing import Any, TypeVar

import msgspec


T = TypeVar("T")


class MsgSpecJsonCodec:

    def __init__(self):
        self.encoder = msgspec.json.Encoder()

    def encode(self, obj: Any) -> bytes:
        return self.encoder.encode(obj)

    def decode(self, data: bytes, type: type[T]) -> T:
        return msgspec.json.decode(data, type=type)

class MsgSpecMsgPackCodec:

    def __init__(self):
        self.encoder = msgspec.msgpack.Encoder()

    def encode(self, obj: Any) -> bytes:
        return self.encoder.encode(obj)

    def decode(self, data: bytes, type: type[T]) -> T:
        return msgspec.msgpack.decode(data, type=type)