"""Byte-level transforms applied around a codec, such as compression."""

from typing import Protocol


class CodecMiddleware(Protocol):
    """Transforms encoded bytes on the way out and reverses it on the way in."""

    def encode(self, data: bytes) -> bytes:
        """Transform outgoing bytes."""

    def decode(self, data: bytes) -> bytes:
        """Reverse ``encode`` on incoming bytes."""