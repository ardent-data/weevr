"""Base model configuration for weevr domain objects."""

from pydantic import BaseModel


class FrozenBase(BaseModel):
    """Immutable base model for all weevr domain objects.

    All domain models inherit from FrozenBase to enforce immutability after
    construction. Mutation attempts raise a ``pydantic.ValidationError``.
    """

    model_config = {"frozen": True}
