from pydantic import BaseModel
import msgspec
from typing import Any, TypeVar, cast

T = TypeVar("T")

class PydanticCodec:
    def encode(self, obj: Any) -> bytes:
        if isinstance(obj, BaseModel):
            return obj.model_dump_json().encode()
        return msgspec.json.encode(obj)

    def decode(self, data: bytes, target: type[T]) -> T:
        if issubclass(target, BaseModel):
            return cast(T, target.model_validate_json(data))
        return msgspec.json.decode(data, type=target)

    def convert(self, obj: Any, target: type[T]) -> T:
        if issubclass(target, BaseModel):
            return cast(T, target.model_validate(obj))
        return msgspec.convert(obj, target)