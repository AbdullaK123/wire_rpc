"""The shape of request middleware."""

from typing import Awaitable, Callable, Optional, Protocol, Any

import msgspec
from wire_rpc.request import WireRequest
from wire_rpc.response import WireResponse


type AppContext = Any
type Next = Callable[[WireRequest, AppContext], Awaitable[WireResponse]]

class Middleware(Protocol):
    """Runs around every handler call.

    Call ``await next(request, ctx)`` to continue to the next middleware or the
    handler, or return a response without calling it to short-circuit.
    """

    async def __call__(self, request: WireRequest, ctx: AppContext, next: Next) -> WireResponse:
        """Handle ``request``, usually by calling ``next`` and returning its response."""
