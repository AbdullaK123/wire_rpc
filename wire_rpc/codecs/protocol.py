from __future__ import annotations

from typing import Any, Protocol, TypeVar

T = TypeVar("T")

class Codec(Protocol):
    def encode(self, obj: Any): 
        ...
    def decode(self, data: bytes, type: type[T]) -> T: 
        ...