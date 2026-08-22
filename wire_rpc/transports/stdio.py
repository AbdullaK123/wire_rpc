import asyncio
from typing import Self
import sys
class StdIoTransport:

    def __init__(self, *cmd: str):
        self._cmd = cmd
        self.proc: asyncio.subprocess.Process | None = None
        self.stdin: asyncio.StreamWriter | None = None
        self.stdout: asyncio.StreamReader | None = None
        self.stderr: asyncio.StreamReader | None = None

    async def connect(self):

        self.proc = await asyncio.create_subprocess_exec(
            *self._cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )

        self.stdin = self.proc.stdin
        self.stdout = self.proc.stdout
        self.stderr = self.proc.stderr

    async def close(self):

        if self.stdin is None or self.proc is None:
            raise RuntimeError("Can not send bytes without calling .connect() first.")

        self.stdin.close()
        await self.stdin.wait_closed()
        await self.proc.wait()

        self.stdin = None
        self.stdout = None
        self.stderr = None
        self.proc = None

    async def recv(self) -> bytes:

        if self.stdout is None:
            raise RuntimeError("Can not receive bytes without calling .connect() first.")
        
        length_bytes = await self.stdout.readexactly(4)
        length = int.from_bytes(length_bytes, "big")
        payload = await self.stdout.readexactly(length)
        return payload

    async def send(self, data: bytes) -> None:

        if self.stdin is None:
            raise RuntimeError("Can not send bytes without calling .connect() first.")

        self.stdin.write(len(data).to_bytes(4, "big"))
        self.stdin.write(data)
        await self.stdin.drain()

    async def __aenter__(self) -> Self:
        await self.connect()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None, 
        exc_val: BaseException | None, 
        exc_tb: object   
    ) -> None:
        await self.close()

class StdIoServerTransport:

    def __init__(self):
        self.writer: asyncio.StreamWriter | None = None
        self.reader: asyncio.StreamReader | None = None

    async def connect(self):

        loop = asyncio.get_event_loop()

        reader = asyncio.StreamReader()
        protocol = asyncio.StreamReaderProtocol(reader)
        await loop.connect_read_pipe(lambda: protocol, sys.stdin.buffer)
        self.reader = reader

        write_transport, _ = await loop.connect_write_pipe(
            asyncio.streams.FlowControlMixin, sys.stdout.buffer
        )

        self.writer = asyncio.StreamWriter(write_transport, protocol, reader, loop)
        

    async def close(self):

        if self.writer is None:
            raise RuntimeError("Can not send bytes without calling .connect() first.")

        self.writer.close()
        await self.writer.wait_closed()

        self.writer = None
        self.reader = None

    async def recv(self) -> bytes:

        if self.reader is None:
            raise RuntimeError("Can not receive bytes without calling .connect() first.")
        
        length_bytes = await self.reader.readexactly(4)
        length = int.from_bytes(length_bytes, "big")
        payload = await self.reader.readexactly(length)
        return payload

    async def send(self, data: bytes) -> None:

        if self.writer is None:
            raise RuntimeError("Can not send bytes without calling .connect() first.")

        self.writer.write(len(data).to_bytes(4, "big"))
        self.writer.write(data)
        await self.writer.drain()

    async def __aenter__(self) -> Self:
        await self.connect()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None, 
        exc_val: BaseException | None, 
        exc_tb: object   
    ) -> None:
        await self.close()


__all__ = [
    "StdIoTransport",
    "StdIoServerTransport"
]