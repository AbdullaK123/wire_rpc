from msgspec import Struct, Meta
from typing import Literal, Optional, Any, Annotated

from wire_rpc.errors import WireError


class WireSuccessResponse[T: Struct](Struct):
    result: T
    id: Optional[str | int] = None
    jsonrpc: Literal["2.0"] = "2.0"
class WireErrorResponse(Struct):
    error: WireError
    id: Optional[str | int] = None
    jsonrpc: Literal["2.0"] = "2.0"

type WireResponse[T: Struct] = WireSuccessResponse[T] | WireErrorResponse