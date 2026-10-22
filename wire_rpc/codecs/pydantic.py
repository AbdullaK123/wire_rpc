"""A JSON codec that validates Pydantic models with Pydantic and everything else with msgspec."""

from typing import Any, get_args

import msgspec
from pydantic import BaseModel, TypeAdapter

from .protocol import CodecConversionError, CodecDecodeError, CodecEncodeError


def _uses_pydantic(target: Any) -> bool:
    """Return True if ``target`` is, or contains, a Pydantic model."""
    if isinstance(target, type) and issubclass(target, BaseModel):
        return True
    return any(_uses_pydantic(arg) for arg in get_args(target))


def _enc_hook(obj: Any) -> Any:
    """Encode Pydantic models that msgspec cannot handle on its own."""
    if isinstance(obj, BaseModel):
        return obj.model_dump(mode="json")
    raise NotImplementedError


class PydanticCodec:
    """JSON codec for handlers whose params or results are Pydantic models."""
    is_text = True
    media_type = "application/json"

    def __init__(self):
        self.encoder = msgspec.json.Encoder(enc_hook=_enc_hook)

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
            if _uses_pydantic(target):
                return TypeAdapter(target).validate_json(data)
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
                return msgspec.convert(
                    msgspec.to_builtins(obj, enc_hook=_enc_hook), dict
                )
            if _uses_pydantic(target):
                return TypeAdapter(target).validate_python(obj)
            return msgspec.convert(obj, target)
        except Exception as exc:
            raise CodecConversionError(str(exc)) from exc


__all__ = ["PydanticCodec"]
