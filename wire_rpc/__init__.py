"""Typed asyncio JSON-RPC 2.0.

Build a server with ``App`` (one peer) or ``MulticastApp`` (many peers), call it
with ``Client``, and pick a transport from ``wire_rpc.transports``. Handlers read
trusted request metadata through ``current_request()``.
"""

from .context import RequestContext, current_request
from .app import App
from .multicast_app import MulticastApp
from .client import Client
from .request import WireRequest
from .response import WireResponse, WireErrorResponse, WireSuccessResponse
from .router import Router

__all__ = [
    "App",
    "RequestContext",
    "current_request",
    "MulticastApp",
    "Client",
    "Router",
    "WireRequest",
    "WireResponse",
    "WireErrorResponse",
    "WireSuccessResponse"
]