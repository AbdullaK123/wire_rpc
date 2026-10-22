"""Codec protocol and normalized codec errors for Wire RPC."""

from __future__ import annotations

from typing import Any, Protocol


class CodecError(Exception):
    """Base error raised by codec implementations."""


class CodecEncodeError(CodecError):
    """Raised when a codec cannot encode an object."""


class CodecDecodeError(CodecError):
    """Raised when wire bytes cannot be decoded into the requested target."""


class CodecConversionError(CodecError):
    """Raised when an in-memory value cannot be converted to the requested target."""


class Codec(Protocol):
    """Turns messages into bytes and back, and converts decoded values to handler types.

    Transports that only carry text check ``is_text`` on the codec, when present.
    """

    def encode(self, obj: Any) -> bytes:
        """Encode ``obj`` to bytes.

        Raises:
            CodecEncodeError: ``obj`` cannot be encoded.
        """

    def decode(self, data: bytes, target: Any) -> Any:
        """Decode ``data`` into an instance of ``target``.

        Raises:
            CodecDecodeError: ``data`` is malformed or does not match ``target``.
        """

    def convert(self, obj: Any, target: Any) -> Any:
        """Convert an already-decoded value to ``target``; ``dict`` turns structs into plain dicts.

        Raises:
            CodecConversionError: ``obj`` does not match ``target``.
        """


__all__ = [
    "Codec",
    "CodecError",
    "CodecEncodeError",
    "CodecDecodeError",
    "CodecConversionError",
]
