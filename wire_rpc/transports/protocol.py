from __future__ import annotations
from typing import Protocol, runtime_checkable
from urllib.parse import ParseResult
from typing_extensions import Self

@runtime_checkable
class StartupComponent(Protocol):
    async def startup(self) -> None: ...
    async def shutdown(self) -> None: ...


class Transport(Protocol):

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


class MulticastTransport(Protocol):

    async def connect(self):
        ...
    async def close(self):
        ...
    async def recv(self) -> tuple[str, bytes]:
        ...
    async def send(self, client_id: str, data: bytes):
        ...
    async def broadcast(self, data: bytes):
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