"""Shared configuration and codec capability checks."""
import math


def positive_timeout(name: str, value: float) -> None:
    """Reject a timeout that is not a finite, positive int or float."""
    if type(value) not in (int,float) or not math.isfinite(value) or value <= 0:
        raise ValueError(f"{name} must be finite and positive")


def positive_limit(name: str, value: int) -> None:
    """Reject a size or count limit that is not a positive int; bools are rejected too."""
    if type(value) is not int or value <= 0:
        raise ValueError(f"{name} must be a positive integer")


def validate_codec(transport, codec) -> None:
    """Reject a binary codec on a transport that can only carry text."""
    if getattr(transport, 'requires_text_codec', False) and not getattr(codec, 'is_text', False):
        raise ValueError('This transport requires a codec declaring is_text=True')
