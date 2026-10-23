# Codecs

A codec turns messages into bytes and back, and converts decoded values into the
types your handlers declare. The client and the server must use the same codec.

```python
from wire_rpc.codecs.msgspec import MsgSpecJsonCodec, MsgSpecMsgPackCodec
from wire_rpc.codecs.pydantic import PydanticCodec

app = App(transport, codec=MsgSpecJsonCodec())        # the default
client = Client(transport, codec=MsgSpecJsonCodec())
```

## Choosing

| Codec | Format | Transports | Params and results can be |
|---|---|---|---|
| `MsgSpecJsonCodec` (default) | JSON | All | msgspec Structs, dataclasses, dicts, lists, scalars |
| `PydanticCodec` | JSON | All | Everything above, plus Pydantic models |
| `MsgSpecMsgPackCodec` | MessagePack | TCP, stdio, threads | msgspec Structs, dataclasses, dicts, lists, scalars |

- Start with **`MsgSpecJsonCodec`**. It's fast, it's what every other JSON-RPC
  client speaks, and it works everywhere.
- Use **`PydanticCodec`** if your models are already Pydantic, or you want
  Pydantic's validators. It's slower than msgspec.
- Use **`MsgSpecMsgPackCodec`** when both ends are wire_rpc, over a binary
  transport, and message size or speed matters. MessagePack is smaller and faster
  to encode than JSON, but it isn't human-readable and most JSON-RPC tools can't
  read it.

`PydanticCodec` and `MsgSpecJsonCodec` both produce plain JSON, so a client using
one can talk to a server using the other.

## Text-only transports

HTTP and WebSocket carry text. MessagePack is binary, so pairing it with them
raises `ValueError` when you build the `App` or `Client`, rather than failing at
the first message:

```python
App(HttpServerTransport("0.0.0.0", 8080), codec=MsgSpecMsgPackCodec())
# ValueError: This transport requires a codec declaring is_text=True
```

## `MsgSpecJsonCodec` and `MsgSpecMsgPackCodec`

Built on [msgspec](https://jcristharif.com/msgspec/). Validation is strict by
default: `"1"` is not accepted where an `int` is declared, unknown Struct fields
are ignored, and missing required fields are errors. Use `msgspec.Meta` for
constraints:

```python
from typing import Annotated
import msgspec
from msgspec import Meta

class Signup(msgspec.Struct):
    email: Annotated[str, Meta(max_length=254, pattern=r"^[^@]+@[^@]+$")]
    age: Annotated[int, Meta(ge=13)]
```

## `PydanticCodec`

Handlers can use Pydantic models for params and results:

```python
from pydantic import BaseModel, Field
from wire_rpc.codecs.pydantic import PydanticCodec

class Signup(BaseModel):
    email: str = Field(max_length=254)
    age: int = Field(ge=13)

app = App(transport, codec=PydanticCodec())

@app.method("signup")
async def signup(params: Signup, ctx) -> Account: ...
```

Types that contain a Pydantic model (including `list[Model]` or
`Model | None`) are validated by Pydantic; everything else goes through msgspec,
so msgspec Structs still work. Pydantic's coercion rules differ from msgspec's
(by default it accepts `"1"` for an `int`), so switching codecs can change what
inputs are accepted. Test before switching, and use explicit, strict models where
it matters.

## Writing your own

A codec is an object with three methods:

```python
from typing import Any

class Codec(Protocol):
    def encode(self, obj: Any) -> bytes: ...
    def decode(self, data: bytes, target: Any) -> Any: ...
    def convert(self, obj: Any, target: Any) -> Any: ...
```

- **`encode`** turns a response, request or batch into bytes. The objects are
  msgspec Structs (`WireSuccessResponse`, `RawWireRequest`, ...) and lists of them.
- **`decode`** parses bytes into `target`. wire_rpc decodes incoming messages with
  `target=Any` and the client decodes replies with `dict`, so it must handle
  those at least.
- **`convert`** turns an already-decoded value into a handler's declared type,
  such as a dict into a Struct. When `target` is `dict`, it must turn Structs into
  plain dicts.

Raise `CodecEncodeError`, `CodecDecodeError` and `CodecConversionError` (from
`wire_rpc.codecs`) for failures. wire_rpc turns them into the right JSON-RPC
errors. Any other exception is a bug in the codec.

Set `is_text = True` if your output is UTF-8 text that HTTP and WebSocket can
carry.

Example: a JSON codec built on the standard library, with msgspec for conversion:

```python
import json
from typing import Any

import msgspec

from wire_rpc.codecs import CodecConversionError, CodecDecodeError, CodecEncodeError


class StdlibJsonCodec:
    is_text = True
    media_type = "application/json"

    def encode(self, obj: Any) -> bytes:
        try:
            return json.dumps(msgspec.to_builtins(obj), separators=(",", ":")).encode()
        except (TypeError, ValueError) as exc:
            raise CodecEncodeError(str(exc)) from exc

    def decode(self, data: bytes, target: Any) -> Any:
        try:
            return msgspec.convert(json.loads(data), target)
        except (ValueError, msgspec.ValidationError) as exc:
            raise CodecDecodeError(str(exc)) from exc

    def convert(self, obj: Any, target: Any) -> Any:
        try:
            if target is dict:
                return msgspec.convert(msgspec.to_builtins(obj), dict)
            return msgspec.convert(obj, target)
        except (TypeError, msgspec.ValidationError) as exc:
            raise CodecConversionError(str(exc)) from exc
```

Don't put the caller's input or your schema into those exception messages if
your codec might be used elsewhere: wire_rpc never sends them to the caller, but
your own logging might record them.
