"""Wire RPC request types.

WireRequest[P] is the typed generic request shape.

RawWireRequest is the framework-level envelope. Params are decoded as
untyped builtins, then converted by the active codec at dispatch time.
"""

from __future__ import annotations

from typing import Any, Literal, Optional

from msgspec import Struct


class WireRequest[P](Struct):
    method: str
    id: Optional[str | int] = None
    params: Optional[P] = None
    jsonrpc: Literal["2.0"] = "2.0"


class RawWireRequest(Struct):
    method: str
    id: Optional[str | int] = None
    params: Optional[Any] = None
    jsonrpc: Literal["2.0"] = "2.0"
