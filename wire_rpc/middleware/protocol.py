from typing import Awaitable, Callable, Protocol, Any
from wire_rpc.request import WireRequest
from wire_rpc.response import WireResponse


type AppContext = Any
type Next = Callable[[WireRequest, AppContext], Awaitable[WireResponse]]

class Middleware(Protocol):
    async def __call__(self, request: WireRequest, ctx: AppContext, next: Next) -> WireResponse:
        ...