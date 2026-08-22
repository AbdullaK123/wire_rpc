"""
Wire RPC request types.

WireRequest[P] — Generic version for handler type hints.
                 Gives typed access to params in handlers.

RawWireRequest — Non-generic version for framework-level
                 decoding. Params decoded as a raw dict,
                 then converted to the typed struct via
                 msgspec.convert at dispatch time.
"""

from __future__ import annotations

from msgspec import Struct
from typing import Literal, Optional, Any


class WireRequest[P: Struct](Struct):
    method: str
    id: Optional[str | int] = None
    params: Optional[P] = None
    jsonrpc: Literal["2.0"] = "2.0"


class RawWireRequest(Struct):
    method: str
    id: Optional[str | int] = None
    params: Optional[Any] = None
    jsonrpc: Literal["2.0"] = "2.0"