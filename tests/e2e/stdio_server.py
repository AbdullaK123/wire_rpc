"""Child process for stdio end-to-end tests: ``python stdio_server.py <codec> [principal]``."""
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]))

from tests.e2e.handlers import install  # noqa: E402
from wire_rpc import App  # noqa: E402
from wire_rpc.codecs.msgspec import MsgSpecJsonCodec, MsgSpecMsgPackCodec  # noqa: E402
from wire_rpc.codecs.pydantic import PydanticCodec  # noqa: E402
from wire_rpc.transports.stdio import StdIoServerTransport  # noqa: E402

CODECS = {'json': MsgSpecJsonCodec, 'msgpack': MsgSpecMsgPackCodec, 'pydantic': PydanticCodec}

if __name__ == '__main__':
    principal = sys.argv[2] if len(sys.argv) > 2 else None
    app = App(StdIoServerTransport(trusted_principal=principal), codec=CODECS[sys.argv[1]]())
    install(app)
    app.run()
