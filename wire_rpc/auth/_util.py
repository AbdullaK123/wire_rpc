import re
from typing import Any
from wire_rpc.auth.errors import AuthUnavailableError
from wire_rpc.transports.protocol import StartupComponent


def identity(value: object) -> str:
    if not isinstance(value, str) or not 1 <= len(value) <= 255 or any(ord(c) < 33 or ord(c) == 127 for c in value):
        raise ValueError('Identity must be a bounded nonempty string without whitespace or controls')
    return value


def token_text(value: object, maximum: int = 4096) -> bool:
    return isinstance(value, str) and 1 <= len(value) <= maximum and re.fullmatch(r'[\x21-\x7e]+', value) is not None


async def validated(validator: Any, value: Any) -> str | None:
    try:
        result = await validator.validate(value)
        return None if result is None else identity(result)
    except AuthUnavailableError:
        raise
    except Exception:
        raise AuthUnavailableError('Authentication backend unavailable') from None


class DependencyLifecycle:
    """Own startup/shutdown of explicitly supplied dependencies, once per lifecycle."""
    def __init__(self, *dependencies: object):
        self._dependencies = dependencies
        self._started: list[StartupComponent] = []
        self._startup_called = False

    async def startup(self) -> None:
        if self._startup_called:
            raise RuntimeError('Authentication already started')
        self._startup_called = True
        try:
            seen: set[int] = set()
            for dependency in self._dependencies:
                if isinstance(dependency, StartupComponent) and id(dependency) not in seen:
                    seen.add(id(dependency))
                    await dependency.startup()
                    self._started.append(dependency)
        except BaseException:
            await self.shutdown()
            raise

    async def shutdown(self) -> None:
        started, self._started = self._started, []
        failure: Exception | None = None
        for dependency in reversed(started):
            try:
                await dependency.shutdown()
            except Exception as exc:
                failure = exc
        if failure is not None:
            raise failure
