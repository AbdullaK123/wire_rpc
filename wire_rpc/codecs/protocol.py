"""Codec protocol and normalized codec errors for Wire RPC."""

from __future__ import annotations

from typing import Any, Protocol, TypeVar

T = TypeVar("T")


class CodecError(Exception):
    """Base error raised by codec implementations."""


class CodecEncodeError(CodecError):
    """Raised when a codec cannot encode an object."""


class CodecDecodeError(CodecError):
    """Raised when wire bytes cannot be decoded into the requested target."""


class CodecConversionError(CodecError):
    """Raised when an in-memory value cannot be converted to the requested target."""


class Codec(Protocol):
    def encode(self, obj: Any) -> bytes:
        ...

    def decode(self, data: bytes, target: Any) -> T:
        ...

    def convert(self, obj: Any, target: Any) -> T:
        ...


__all__ = [
    "Codec",
    "CodecError",
    "CodecEncodeError",
    "CodecDecodeError",
    "CodecConversionError",
]
