"""Wire RPC request types.

WireRequest[P] is the typed generic request shape.

RawWireRequest is the framework-level envelope. Params are decoded as
untyped builtins, then converted by the active codec at dispatch time.
"""

from __future__ import annotations

from typing import Any, Literal, Optional

from msgspec import Struct, UNSET, UnsetType, Meta
from typing import Annotated


class WireRequest[P](Struct):
    """A JSON-RPC 2.0 request whose ``params`` are typed as ``P``. Leave ``id`` unset to send a notification."""
    method: Annotated[str, Meta(min_length=1, max_length=255)]
    id: Annotated[str, Meta(max_length=255)] | Annotated[int, Meta(ge=-(2**63), le=2**63-1)] | None | UnsetType = UNSET
    params: Optional[P] = None
    jsonrpc: Literal["2.0"] = "2.0"


class RawWireRequest(Struct):
    """A JSON-RPC 2.0 request with undecoded ``params``; the server converts them per handler."""
    method: Annotated[str, Meta(min_length=1, max_length=255)]
    id: Annotated[str, Meta(max_length=255)] | Annotated[int, Meta(ge=-(2**63), le=2**63-1)] | None | UnsetType = UNSET
    params: Optional[Any] = None
    jsonrpc: Literal["2.0"] = "2.0"

    @property
    def response_id(self) -> str | int | None:
        """Normalize notification IDs for internal response construction."""
        return None if isinstance(self.id, UnsetType) else self.id
