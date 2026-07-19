"""Lookup metadata carrier — materialization-time facts for resolve steps.

The engine learns things about a lookup when it materializes it (declared
key columns, unique-key check outcome, source size). Resolve steps can
reuse those facts to skip redundant work — but only the reduced facts
travel, not the engine's ``LookupResult``, keeping the operations layer
free of engine imports.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class LookupMeta:
    """Reduced materialization facts for one lookup.

    Attributes:
        key_columns: The lookup's declared ``key`` columns, if any.
        unique_key_checked: Whether a unique-key check ran at
            materialization.
        unique_key_passed: The check's outcome, when it ran.
        size_in_bytes: Delta snapshot size of the lookup's source table,
            when known — evidence for the broadcast hint policy.
    """

    key_columns: tuple[str, ...] | None = None
    unique_key_checked: bool = False
    unique_key_passed: bool | None = None
    size_in_bytes: int | None = None
