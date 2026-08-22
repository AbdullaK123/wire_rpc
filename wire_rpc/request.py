from msgspec import Struct
from typing import Literal, Optional


class WireRequest[P: Struct](Struct):
    method: str
    id: Optional[str | int] = None
    params: Optional[P] = None
    jsonrpc: Literal["2.0"] = "2.0"