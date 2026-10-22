"""JSON-RPC 2.0 response envelopes."""

from typing import Literal, Optional

from msgspec import Struct

from wire_rpc.errors import WireError


class WireSuccessResponse[T](Struct):
    """A successful response carrying the handler's ``result``."""
    result: T
    id: Optional[str | int] = None
    jsonrpc: Literal["2.0"] = "2.0"


class WireErrorResponse(Struct):
    """A failed response carrying one of the ``WireError`` types."""
    error: WireError
    id: Optional[str | int] = None
    jsonrpc: Literal["2.0"] = "2.0"


type WireResponse[T] = WireSuccessResponse[T] | WireErrorResponse
