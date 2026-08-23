from typing import Any, get_args

import msgspec
from pydantic import BaseModel, TypeAdapter

from .protocol import CodecConversionError, CodecDecodeError, CodecEncodeError


def _uses_pydantic(target: Any) -> bool:
    if isinstance(target, type) and issubclass(target, BaseModel):
        return True
    return any(_uses_pydantic(arg) for arg in get_args(target))


def _enc_hook(obj: Any) -> Any:
    if isinstance(obj, BaseModel):
        return obj.model_dump(mode="json")
    raise NotImplementedError


class PydanticCodec:

    def __init__(self):
        self.encoder = msgspec.json.Encoder(enc_hook=_enc_hook)

    def encode(self, obj: Any) -> bytes:
        try:
            return self.encoder.encode(obj)
        except Exception as exc:
            raise CodecEncodeError(str(exc)) from exc

    def decode(self, data: bytes, target: Any) -> Any:
        try:
            if _uses_pydantic(target):
                return TypeAdapter(target).validate_json(data)
            return msgspec.json.decode(data, type=target)
        except Exception as exc:
            raise CodecDecodeError(str(exc)) from exc

    def convert(self, obj: Any, target: Any) -> Any:
        try:
            if target is dict:
                return msgspec.to_builtins(obj, enc_hook=_enc_hook)
            if _uses_pydantic(target):
                return TypeAdapter(target).validate_python(obj)
            return msgspec.convert(obj, target)
        except Exception as exc:
            raise CodecConversionError(str(exc)) from exc


__all__ = ["PydanticCodec"]
