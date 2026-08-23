from typing import Literal, Optional

from msgspec import Struct

from wire_rpc.errors import WireError


class WireSuccessResponse[T](Struct):
    result: T
    id: Optional[str | int] = None
    jsonrpc: Literal["2.0"] = "2.0"


class WireErrorResponse(Struct):
    error: WireError
    id: Optional[str | int] = None
    jsonrpc: Literal["2.0"] = "2.0"


type WireResponse[T] = WireSuccessResponse[T] | WireErrorResponse
