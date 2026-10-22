"""Codecs: how messages become bytes.

Use ``MsgSpecJsonCodec`` (default), ``MsgSpecMsgPackCodec`` or ``PydanticCodec``.
Client and server must use the same codec.
"""

from .protocol import (
    Codec,
    CodecConversionError,
    CodecDecodeError,
    CodecEncodeError,
    CodecError,
)

__all__ = [
    "Codec",
    "CodecError",
    "CodecEncodeError",
    "CodecDecodeError",
    "CodecConversionError",
]
