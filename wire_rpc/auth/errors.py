class AuthUnavailableError(Exception):
    """Authentication could not be decided; never treat as anonymous success."""


class SessionCapacityError(AuthUnavailableError):
    """The session store cannot admit another live session."""
