from typing import Any

import msgspec

from .protocol import CodecConversionError, CodecDecodeError, CodecEncodeError


def _to_builtins(obj: Any) -> Any:
    return msgspec.to_builtins(obj)


class MsgSpecJsonCodec:

    def __init__(self):
        self.encoder = msgspec.json.Encoder()

    def encode(self, obj: Any) -> bytes:
        try:
            return self.encoder.encode(obj)
        except Exception as exc:
            raise CodecEncodeError(str(exc)) from exc

    def decode(self, data: bytes, target: Any) -> Any:
        try:
            return msgspec.json.decode(data, type=target)
        except Exception as exc:
            raise CodecDecodeError(str(exc)) from exc

    def convert(self, obj: Any, target: Any) -> Any:
        try:
            if target is dict:
                return _to_builtins(obj)
            return msgspec.convert(obj, target)
        except Exception as exc:
            raise CodecConversionError(str(exc)) from exc


class MsgSpecMsgPackCodec:

    def __init__(self):
        self.encoder = msgspec.msgpack.Encoder()

    def encode(self, obj: Any) -> bytes:
        try:
            return self.encoder.encode(obj)
        except Exception as exc:
            raise CodecEncodeError(str(exc)) from exc

    def decode(self, data: bytes, target: Any) -> Any:
        try:
            return msgspec.msgpack.decode(data, type=target)
        except Exception as exc:
            raise CodecDecodeError(str(exc)) from exc

    def convert(self, obj: Any, target: Any) -> Any:
        try:
            if target is dict:
                return _to_builtins(obj)
            return msgspec.convert(obj, target)
        except Exception as exc:
            raise CodecConversionError(str(exc)) from exc


__all__ = [
    "MsgSpecJsonCodec",
    "MsgSpecMsgPackCodec",
]
