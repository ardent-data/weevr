"""Lookup model for weave-level named data definitions."""

from typing import Literal

from pydantic import Field, model_validator

from weevr.model.base import FrozenBase
from weevr.model.source import Source


class Lookup(FrozenBase):
    """A weave-level named data definition that can be referenced by threads.

    Lookups define small reference datasets shared across threads in a weave.
    When ``materialize`` is true, the data is read once and cached (or broadcast)
    before threads execute.

    Narrow lookup fields (``key``, ``values``, ``filter``) control projection
    and filtering applied during materialization or on-demand reads.  When set,
    only the declared key and value columns are retained, reducing memory and
    improving join performance.

    Attributes:
        source: Source definition for the lookup data.
        materialize: Whether to pre-read and cache/broadcast the data before
            thread execution.
        strategy: Materialization strategy. Only meaningful when ``materialize``
            is true. ``"broadcast"`` uses Spark broadcast join hints;
            ``"cache"`` uses DataFrame caching.
        key: Column(s) used for matching (join key). Kept in the cached
            projection alongside ``values``.
        values: Payload column(s) to retrieve. When set, only ``key`` + ``values``
            columns are retained in the cached DataFrame.
        filter: SQL WHERE expression applied to the source before projection.
        unique_key: When true, validate that ``key`` columns form a unique key
            after filtering and projection.
        on_failure: Behavior when ``unique_key`` check finds duplicates.
            Only meaningful when ``unique_key`` is true.
    """

    source: Source = Field(description="Source definition for the lookup data.")
    materialize: bool = Field(
        default=False,
        description="Whether to pre-read and cache or broadcast the data before thread execution.",
    )
    strategy: Literal["broadcast", "cache"] = Field(
        default="cache",
        description=(
            "Materialization strategy when ``materialize`` is true."
            " ``broadcast`` uses Spark broadcast join hints; ``cache`` uses DataFrame caching."
        ),
    )

    # Narrow lookup fields
    key: list[str] | None = Field(
        default=None,
        description=(
            "Column(s) used for matching (join key). Retained in the cached projection"
            " alongside ``values``."
        ),
    )
    values: list[str] | None = Field(
        default=None,
        description=(
            "Payload column(s) to retrieve. When set, only ``key`` + ``values`` columns are"
            " retained in the cached DataFrame."
        ),
    )
    filter: str | None = Field(
        default=None,
        description="SQL WHERE expression applied to the source before projection.",
    )
    unique_key: bool = Field(
        default=False,
        description="When true, validate that ``key`` columns form a unique key after filtering.",
    )
    on_failure: Literal["abort", "warn"] = Field(
        default="abort",
        description=(
            "Behavior when the ``unique_key`` check finds duplicates."
            " Only meaningful when ``unique_key`` is true."
        ),
    )

    @model_validator(mode="after")
    def _validate_narrow_fields(self) -> "Lookup":
        if self.values and not self.key:
            raise ValueError("'values' requires 'key' to be set")
        if self.key and self.values:
            overlap = set(self.key) & set(self.values)
            if overlap:
                raise ValueError(f"'key' and 'values' must not overlap: {sorted(overlap)}")
        if self.on_failure != "abort" and not self.unique_key:
            raise ValueError("'on_failure' is only valid when 'unique_key' is true")
        return self
