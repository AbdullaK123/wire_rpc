"""
Codec protocol for Wire RPC.

Three methods:
    encode  — object → bytes (for the wire)
    decode  — bytes → typed object (from the wire)
    convert — untyped object (dict/list) → typed object (for validation)
"""

from __future__ import annotations

from typing import Any, Protocol, TypeVar

T = TypeVar("T")

class Codec(Protocol):
    def encode(self, obj: Any) -> bytes: 
        ...
    def decode(self, data: bytes, target: type[T]) -> T: 
        ...
    def convert(self, obj: Any, target: type[T]) -> T:
        ...