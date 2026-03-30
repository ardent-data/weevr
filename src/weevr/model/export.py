"""Export domain model — secondary output destinations for thread data."""

from typing import Literal

from pydantic import Field, model_validator

from weevr.model.base import FrozenBase

# Supported export formats — all Fabric-native Spark formats.
ExportFormat = Literal["delta", "parquet", "csv", "json", "orc"]


class Export(FrozenBase):
    """A named secondary output destination for thread data.

    Exports write the same post-mapping, audit-injected DataFrame as the
    primary target to additional locations in configurable formats.

    Attributes:
        name: Unique identifier within the resolved thread.
        description: Optional human-readable label shown in explain() output.
        type: Output format (delta, parquet, csv, json, orc).
        path: OneLake path for the export. Supports context variables.
        alias: Metastore alias (delta type only, mutually exclusive with path).
        mode: Write mode. Only ``"overwrite"`` in v1.
        partition_by: Partition columns, independent of primary target.
        on_failure: Behavior on write error — ``"abort"`` fails the thread,
            ``"warn"`` logs and continues.
        enabled: Set to ``False`` to suppress an inherited export.
        options: Format-specific Spark DataFrameWriter options.
    """

    name: str = Field(description="Unique identifier within the resolved thread.")
    description: str | None = Field(
        default=None,
        description="Optional human-readable label shown in explain() output.",
    )
    type: ExportFormat = Field(
        description="Output format (delta, parquet, csv, json, orc).",
    )
    path: str | None = Field(
        default=None,
        description="OneLake path for the export. Supports context variables.",
    )
    alias: str | None = Field(
        default=None,
        description="Metastore alias (delta type only, mutually exclusive with path).",
    )
    mode: Literal["overwrite"] = Field(
        default="overwrite",
        description="Write mode. Only ``overwrite`` is supported in v1.",
    )
    partition_by: list[str] | None = Field(
        default=None,
        description="Partition columns, independent of the primary target.",
    )
    on_failure: Literal["abort", "warn"] = Field(
        default="warn",
        description=(
            "Behavior on write error. ``abort`` fails the thread; ``warn`` logs and continues."
        ),
    )
    enabled: bool = Field(
        default=True,
        description="Set to ``False`` to suppress an inherited export.",
    )
    options: dict[str, str] | None = Field(
        default=None,
        description="Format-specific Spark DataFrameWriter options.",
    )

    @model_validator(mode="after")
    def _validate_export(self) -> "Export":
        """Enforce cross-field constraints.

        - ``alias`` requires ``type == "delta"``.
        - Exactly one of ``path`` or ``alias`` must be set when enabled.
        - ``name`` must be a valid Python identifier.
        """
        if not self.name.isidentifier():
            raise ValueError(f"Export name '{self.name}' is not a valid identifier")

        if self.alias is not None and self.type != "delta":
            raise ValueError(
                f"Export '{self.name}': alias is only valid for delta type, got type='{self.type}'"
            )

        if self.enabled:
            has_path = bool(self.path)
            has_alias = bool(self.alias)
            if has_path == has_alias:
                raise ValueError(
                    f"Export '{self.name}': exactly one of 'path' or 'alias' "
                    f"must be set (got path={has_path}, alias={has_alias})"
                )

        return self
