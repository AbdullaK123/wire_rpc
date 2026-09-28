import pytest

from wire_rpc.codecs import CodecConversionError, CodecDecodeError, CodecEncodeError
from wire_rpc.codecs.msgspec import MsgSpecJsonCodec, MsgSpecMsgPackCodec
from wire_rpc.codecs.pydantic import PydanticCodec


@pytest.fixture(params=[MsgSpecJsonCodec, MsgSpecMsgPackCodec, PydanticCodec])
def codec(request):
    return request.param()


@pytest.mark.parametrize("value", [[], [1], "secret", 42, None])
def test_dict_conversion_cannot_silently_return_a_non_mapping(codec, value):
    with pytest.raises(CodecConversionError):
        codec.convert(value, dict)


@pytest.mark.parametrize("data", [b"", b"\xc1", b"{", b'{}{}'])
def test_malformed_wire_bytes_raise_the_public_decode_error(codec, data):
    with pytest.raises(CodecDecodeError):
        codec.decode(data, dict)


def test_unsupported_objects_raise_the_public_encode_error(codec):
    with pytest.raises(CodecEncodeError):
        codec.encode(object())
