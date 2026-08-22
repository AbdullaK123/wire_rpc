from typing import Awaitable, Callable, Optional, Protocol, Any

import msgspec
from wire_rpc.request import WireRequest
from wire_rpc.response import WireResponse


type AppContext = Any
type Next = Callable[[WireRequest, AppContext], Awaitable[WireResponse]]

class Handler[P: msgspec.Struct, T: msgspec.Struct](Protocol):
    def __call__(
        self, 
        request: WireRequest[P], 
        context: AppContext
    ) -> Awaitable[T]:
        ...

class Middleware(Protocol):
    async def __call__(self, request: WireRequest, ctx: AppContext, next: Next) -> WireResponse:
        ...