from __future__ import annotations
from typing import Protocol
from urllib.parse import ParseResult
from typing_extensions import Self


class Transport(Protocol):

    @classmethod
    def from_uri(cls, parsed: ParseResult) -> Self:
        ...
    async def connect(self): 
        ...
    async def close(self): 
        ...
    async def recv(self) -> bytes: 
        ...
    async def send(self, data: bytes): 
        ...
    async def __aenter__(self) -> Self: 
        ...
    async def __aexit__(
        self, 
        exc_type: type[BaseException] | None, 
        exc_val: BaseException | None, 
        exc_tb: object
    ) -> None: 
        ...