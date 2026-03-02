"""Lookup model for weave-level named data definitions."""

from typing import Literal

from weevr.model.base import FrozenBase
from weevr.model.source import Source


class Lookup(FrozenBase):
    """A weave-level named data definition that can be referenced by threads.

    Lookups define small reference datasets shared across threads in a weave.
    When ``materialize`` is true, the data is read once and cached (or broadcast)
    before threads execute.

    Attributes:
        source: Source definition for the lookup data.
        materialize: Whether to pre-read and cache/broadcast the data before
            thread execution.
        strategy: Materialization strategy. Only meaningful when ``materialize``
            is true. ``"broadcast"`` uses Spark broadcast join hints;
            ``"cache"`` uses DataFrame caching.
    """

    source: Source
    materialize: bool = False
    strategy: Literal["broadcast", "cache"] = "cache"
