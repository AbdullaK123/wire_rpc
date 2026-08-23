from typing import Protocol, Any

class CredentialValidator(Protocol):
 
    async def validate(self, credentials: dict[str, Any]) -> str | None:
        ...