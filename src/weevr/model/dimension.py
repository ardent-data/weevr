"""Dimension target configuration models."""

from datetime import date
from typing import Literal

from pydantic import field_validator, model_validator

from weevr.model.base import FrozenBase

_ALGORITHM_LITERAL = Literal[
    "xxhash64", "sha1", "sha256", "sha384", "sha512", "md5", "crc32", "murmur3"
]


class SurrogateKeyConfig(FrozenBase):
    """Surrogate key generation configuration for a dimension target.

    Unlike the keys.py SurrogateKeyConfig, this variant includes a ``columns``
    field that specifies which source columns are hashed to produce the SK.

    Attributes:
        name: Output column name for the generated surrogate key.
        algorithm: Hash algorithm to use.
        columns: Source columns to hash into the surrogate key value.
        output: Controls the output type for integer-returning algorithms
            (xxhash64, crc32, murmur3). ``native`` preserves the algorithm's
            return type; ``string`` casts to StringType.
    """

    name: str
    algorithm: _ALGORITHM_LITERAL = "sha256"
    columns: list[str]
    output: Literal["native", "string"] = "native"


class ChangeDetectionGroupConfig(FrozenBase):
    """Configuration for a single change detection group within a dimension.

    Groups may be named (part of a named-groups map) or anonymous. When
    ``columns`` is ``"auto"``, the engine selects all non-key source columns
    not already claimed by another group.

    Attributes:
        name: Optional display name for the group (used as output column name
            when not overridden by the dict key in DimensionConfig).
        algorithm: Hash algorithm. When ``None``, the engine applies a default.
        columns: Source columns for this group's hash, or ``"auto"`` to let the
            engine select the remaining unclaimed columns.
        on_change: Action when the hash changes: ``version`` creates a new SCD
            row (requires ``track_history: true``), ``overwrite`` updates in
            place, ``static`` marks the group as non-versioned metadata.
        output: Controls the output type for integer-returning algorithms.
    """

    name: str | None = None
    algorithm: _ALGORITHM_LITERAL | None = None
    columns: list[str] | Literal["auto"]
    on_change: Literal["version", "overwrite", "static"]
    output: Literal["native", "string"] = "native"


class AdditionalKeyConfig(FrozenBase):
    """Configuration for an additional (non-surrogate) generated key.

    Attributes:
        name: Output column name for the generated key.
        algorithm: Hash algorithm to use.
        columns: Source columns to hash into the key value.
        output: Controls the output type for integer-returning algorithms.
    """

    name: str
    algorithm: _ALGORITHM_LITERAL = "sha256"
    columns: list[str]
    output: Literal["native", "string"] = "native"


class SystemMemberConfig(FrozenBase):
    """Configuration for a single system-reserved dimension member.

    System members occupy negative SK space and represent well-known sentinel
    rows such as "Unknown" or "Not Applicable".

    Attributes:
        sk: Surrogate key value for this system member. Must be negative.
        code: Short business code identifying the member (e.g. ``"UNKNOWN"``).
        label: Human-readable description (e.g. ``"Unknown"``).
    """

    sk: int
    code: str
    label: str

    @field_validator("sk")
    @classmethod
    def sk_must_be_negative(cls, v: int) -> int:
        """Enforce that all system member SK values are negative integers."""
        if v >= 0:
            raise ValueError(f"system member sk must be negative, got {v}")
        return v


class ScdColumnConfig(FrozenBase):
    """SCD tracking column name configuration.

    Attributes:
        valid_from: Column name for the row effective-from timestamp.
        valid_to: Column name for the row effective-to timestamp.
        is_current: Column name for the current-row flag.
    """

    valid_from: str = "_valid_from"
    valid_to: str = "_valid_to"
    is_current: str = "_is_current"


class ScdDateConfig(FrozenBase):
    """Boundary dates used in SCD valid-from / valid-to columns.

    Attributes:
        min: Earliest valid-from date for the first version of a row,
            in ``YYYY-MM-DD`` format.
        max: Sentinel valid-to date for currently active rows,
            in ``YYYY-MM-DD`` format.
    """

    min: str = "1970-01-01"
    max: str = "9999-12-31"

    @model_validator(mode="after")
    def min_before_max(self) -> "ScdDateConfig":
        """Validate that min is strictly before max."""
        try:
            min_date = date.fromisoformat(self.min)
            max_date = date.fromisoformat(self.max)
        except ValueError as exc:
            raise ValueError(f"dates must be in YYYY-MM-DD format: {exc}") from exc
        if min_date >= max_date:
            raise ValueError(f"dates.min ({self.min}) must be before dates.max ({self.max})")
        return self


class DimensionConfig(FrozenBase):
    """Full configuration for a dimension analytical target.

    Defines the surrogate key, business key, optional SCD tracking, change
    detection groups, additional keys, system members, and SCD column/date
    boundaries for a dimension table produced by the weevr engine.

    Attributes:
        business_key: One or more source columns that form the natural key.
        surrogate_key: Surrogate key generation configuration.
        track_history: When ``True``, the target is an SCD Type 2 table and
            groups with ``on_change: version`` produce new history rows.
        previous_columns: Map of output column name → source column name for
            capturing the previous value of selected attributes.
        change_detection: Named map of change detection groups. Each key is the
            group name (used as the output hash column name unless the group
            config provides its own ``name``).
        additional_keys: Named map of additional generated key configurations.
        columns: SCD tracking column names. Defaults to ``_valid_from``,
            ``_valid_to``, ``_is_current``.
        dates: SCD boundary dates. Defaults to ``1970-01-01`` / ``9999-12-31``.
        seed_system_members: When ``True``, the engine inserts ``system_members``
            rows during the first load.
        system_members: Reserved sentinel rows with negative SK values.
        label_column: Optional column name for a human-readable row label.
        history_filter: When ``True`` (default), the engine exposes a filtered
            view that returns only current rows.
    """

    business_key: list[str]
    surrogate_key: SurrogateKeyConfig
    track_history: bool = False
    previous_columns: dict[str, str] | None = None
    change_detection: dict[str, ChangeDetectionGroupConfig] | None = None
    additional_keys: dict[str, AdditionalKeyConfig] | None = None
    columns: ScdColumnConfig = ScdColumnConfig()
    dates: ScdDateConfig = ScdDateConfig()
    seed_system_members: bool = False
    system_members: list[SystemMemberConfig] | None = None
    label_column: str | None = None
    history_filter: bool = True

    @model_validator(mode="after")
    def validate_business_key_non_empty(self) -> "DimensionConfig":
        """Validate that business_key contains at least one column."""
        if not self.business_key:
            raise ValueError("business_key must contain at least one column")
        return self

    @model_validator(mode="after")
    def validate_surrogate_key_columns_non_empty(self) -> "DimensionConfig":
        """Validate that surrogate_key.columns contains at least one column."""
        if not self.surrogate_key.columns:
            raise ValueError("surrogate_key.columns must contain at least one column")
        return self

    @model_validator(mode="after")
    def validate_change_detection_groups(self) -> "DimensionConfig":
        """Validate cross-group constraints on change_detection.

        Rules enforced:
        - No column may appear in more than one explicit (non-auto) group.
        - At most one group may use ``columns: auto``.
        - When ``track_history`` is ``False``, no group may use
          ``on_change: version``.
        """
        if not self.change_detection:
            return self

        seen_columns: set[str] = set()
        auto_count = 0

        for group_name, group in self.change_detection.items():
            if self.track_history is False and group.on_change == "version":
                raise ValueError(
                    f"change_detection group '{group_name}' has on_change: version "
                    f"but track_history is False — enable track_history to use versioning"
                )

            if group.columns == "auto":
                auto_count += 1
                if auto_count > 1:
                    raise ValueError("at most one change_detection group may use columns: auto")
            else:
                for col in group.columns:  # type: ignore[union-attr]
                    if col in seen_columns:
                        raise ValueError(
                            f"column '{col}' appears in multiple change_detection groups"
                        )
                    seen_columns.add(col)

        return self
