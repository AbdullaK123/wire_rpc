"""The JSON-RPC 2.0 error objects, one class per reserved error code."""

from typing import Annotated, Any, Literal, Optional

from msgspec import Meta, Struct


class ParseError(Struct):
    """-32700: the request bytes could not be decoded."""
    message: str
    data: Optional[Any] = None
    code: Literal[-32700] = -32700

class InvalidRequestError(Struct):
    """-32600: the request decoded but is not a valid JSON-RPC 2.0 request."""
    message: str
    data: Optional[Any] = None
    code: Literal[-32600] = -32600

class MethodNotFoundError(Struct):
    """-32601: no handler is registered for the method."""
    message: str
    data: Optional[Any] = None
    code: Literal[-32601] = -32601

class InvalidParamsError(Struct):
    """-32602: the params do not match the handler's declared type."""
    message: str
    data: Optional[Any] = None
    code: Literal[-32602] = -32602

class InternalError(Struct):
    """-32603: the handler or response encoding failed; details are never sent to the caller."""
    message: str
    data: Optional[Any] = None
    code: Literal[-32603] = -32603

class ServerError(Struct):
    """-32000 to -32099: a server-defined error such as a timeout or denied access."""
    message: str
    code: Annotated[int, Meta(ge=-32099, le=-32000)]
    data: Optional[Any] = None

type WireError = (
    ParseError |
    InvalidRequestError | 
    MethodNotFoundError | 
    InvalidParamsError | 
    InternalError | 
    ServerError
)

__all__ = [
    "ParseError",
    "InvalidRequestError",
    "MethodNotFoundError",
    "InvalidParamsError",
    "InternalError",
    "ServerError",
    "WireError"
]