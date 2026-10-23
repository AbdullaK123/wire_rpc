"""RPC methods every end-to-end server exposes, shared with the stdio child process."""
import msgspec

from wire_rpc import current_request


class AddParams(msgspec.Struct):
    a: int
    b: int


def install(app, *, multicast=False):
    """Register the shared methods on ``app`` and return the list notifications append to."""
    notes: list[object] = []

    @app.method('add')
    async def add(params: AddParams, ctx) -> int:
        return params.a + params.b

    @app.method('echo')
    async def echo(params: dict, ctx) -> dict:
        return params

    @app.method('whoami')
    async def whoami(ctx) -> str | None:
        return current_request().principal

    @app.method('fail')
    async def fail(ctx):
        raise ValueError('SECRET-db-password-hunter2')

    @app.method('note')
    async def note(params: dict, ctx) -> None:
        notes.append(params.get('n'))

    @app.method('notes')
    async def seen(ctx) -> list:
        return list(notes)

    return notes
