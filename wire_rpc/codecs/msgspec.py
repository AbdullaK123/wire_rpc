from typing import Any, TypeVar

import msgspec


T = TypeVar("T")


class MsgSpecJsonCodec:

    def __init__(self):
        self.encoder = msgspec.json.Encoder()

    def encode(self, obj: Any) -> bytes:
        return self.encoder.encode(obj)

    def decode(self, data: bytes, target: type[T]) -> T:
        return msgspec.json.decode(data, type=target)

    def convert(self, obj: Any, target: type[T]) -> T:
        if target is dict:
            return msgspec.to_builtins(obj)  # type: ignore
        return msgspec.convert(obj, target)


class MsgSpecMsgPackCodec:

    def __init__(self):
        self.encoder = msgspec.msgpack.Encoder()

    def encode(self, obj: Any) -> bytes:
        return self.encoder.encode(obj)

    def decode(self, data: bytes, target: type[T]) -> T:
        return msgspec.msgpack.decode(data, type=target)

    def convert(self, obj: Any, target: type[T]) -> T:
        if target is dict:
            return msgspec.to_builtins(obj)  # type: ignore
        return msgspec.convert(obj, target)


__all__ = [
    "MsgSpecJsonCodec",
    "MsgSpecMsgPackCodec"
]