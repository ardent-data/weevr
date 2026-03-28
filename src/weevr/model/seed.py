"""Seed configuration model for target seed rows."""

from typing import Any, Literal

from pydantic import field_validator

from weevr.model.base import FrozenBase


class SeedConfig(FrozenBase):
    """Configuration for seed rows to insert into a target table.

    Declares seed rows that should be inserted into a table on first write
    or when the table is empty, enabling population of reference data and
    baseline records.

    Attributes:
        on: Trigger condition for seed insertion. ``"first_write"`` inserts
            on initial table creation; ``"empty"`` inserts whenever the table
            is empty. Defaults to ``"first_write"``.
        rows: Non-empty list of row dicts to insert. Each dict is a complete
            row with column names as keys and cell values as values.
    """

    on: Literal["first_write", "empty"] = "first_write"
    rows: list[dict[str, Any]]

    @field_validator("rows")
    @classmethod
    def _validate_rows_non_empty(cls, v: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Ensure rows list is non-empty."""
        if not v:
            raise ValueError("rows must contain at least one row")
        return v
