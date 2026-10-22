"""Codecs built on msgspec: JSON and MessagePack."""

from typing import Any

import msgspec

from .protocol import CodecConversionError, CodecDecodeError, CodecEncodeError


def _to_builtins(obj: Any) -> Any:
    """Turn structs and other msgspec-supported objects into plain Python values."""
    return msgspec.to_builtins(obj)


class MsgSpecJsonCodec:
    """JSON via msgspec. The default codec; works on every transport."""
    is_text = True
    media_type = "application/json"

    def __init__(self):
        self.encoder = msgspec.json.Encoder()

    def encode(self, obj: Any) -> bytes:
        """Encode ``obj`` to bytes.

        Raises:
            CodecEncodeError: ``obj`` cannot be encoded.
        """
        try:
            return self.encoder.encode(obj)
        except Exception as exc:
            raise CodecEncodeError(str(exc)) from exc

    def decode(self, data: bytes, target: Any) -> Any:
        """Decode ``data`` into an instance of ``target``.

        Raises:
            CodecDecodeError: ``data`` is malformed or does not match ``target``.
        """
        try:
            return msgspec.json.decode(data, type=target)
        except Exception as exc:
            raise CodecDecodeError(str(exc)) from exc

    def convert(self, obj: Any, target: Any) -> Any:
        """Convert an already-decoded value to ``target``; ``dict`` turns structs into plain dicts.

        Raises:
            CodecConversionError: ``obj`` does not match ``target``.
        """
        try:
            if target is dict:
                return msgspec.convert(_to_builtins(obj), dict)
            return msgspec.convert(obj, target)
        except Exception as exc:
            raise CodecConversionError(str(exc)) from exc


class MsgSpecMsgPackCodec:
    """MessagePack via msgspec. Smaller and faster than JSON, but binary, so not usable on HTTP or WebSocket."""
    is_text = False
    media_type = "application/msgpack"

    def __init__(self):
        self.encoder = msgspec.msgpack.Encoder()

    def encode(self, obj: Any) -> bytes:
        """Encode ``obj`` to bytes.

        Raises:
            CodecEncodeError: ``obj`` cannot be encoded.
        """
        try:
            return self.encoder.encode(obj)
        except Exception as exc:
            raise CodecEncodeError(str(exc)) from exc

    def decode(self, data: bytes, target: Any) -> Any:
        """Decode ``data`` into an instance of ``target``.

        Raises:
            CodecDecodeError: ``data`` is malformed or does not match ``target``.
        """
        try:
            return msgspec.msgpack.decode(data, type=target)
        except Exception as exc:
            raise CodecDecodeError(str(exc)) from exc

    def convert(self, obj: Any, target: Any) -> Any:
        """Convert an already-decoded value to ``target``; ``dict`` turns structs into plain dicts.

        Raises:
            CodecConversionError: ``obj`` does not match ``target``.
        """
        try:
            if target is dict:
                return msgspec.convert(_to_builtins(obj), dict)
            return msgspec.convert(obj, target)
        except Exception as exc:
            raise CodecConversionError(str(exc)) from exc


__all__ = [
    "MsgSpecJsonCodec",
    "MsgSpecMsgPackCodec",
]
