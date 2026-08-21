from __future__ import annotations
from typing import Protocol
from typing_extensions import Self


class Transport(Protocol):
    async def connect(self): 
        ...
    async def close(self): 
        ...
    async def recv(self): 
        ...
    async def send(self, data: bytes): 
        ...
    async def __aenter__(self) -> Self: 
        ...
    async def __aclose__(
        self, 
        exc_type: type[BaseException] | None, 
        exc_val: BaseException | None, 
        exc_tb: object
    ) -> None: 
        ...