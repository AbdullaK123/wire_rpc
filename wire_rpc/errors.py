from typing import Annotated, Any, Literal, Optional

from msgspec import Meta, Struct


class ParseError(Struct):
    message: str
    data: Optional[Any] = None
    code: Literal[-32700] = -32700

class InvalidRequestError(Struct):
    message: str
    data: Optional[Any] = None
    code: Literal[-32600] = -32600

class MethodNotFoundError(Struct):
    message: str
    data: Optional[Any] = None
    code: Literal[-32601] = -32601

class InvalidParamsError(Struct):
    message: str
    data: Optional[Any] = None
    code: Literal[-32602] = -32602

class InternalError(Struct):
    message: str
    data: Optional[Any] = None
    code: Literal[-32603] = -32603

class ServerError(Struct):
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