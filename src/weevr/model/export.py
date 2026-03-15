"""Export domain model — secondary output destinations for thread data."""

from typing import Literal

from pydantic import model_validator

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

    name: str
    description: str | None = None
    type: ExportFormat
    path: str | None = None
    alias: str | None = None
    mode: Literal["overwrite"] = "overwrite"
    partition_by: list[str] | None = None
    on_failure: Literal["abort", "warn"] = "warn"
    enabled: bool = True
    options: dict[str, str] | None = None

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
            has_path = self.path is not None
            has_alias = self.alias is not None
            if has_path == has_alias:
                raise ValueError(
                    f"Export '{self.name}': exactly one of 'path' or 'alias' "
                    f"must be set (got path={has_path}, alias={has_alias})"
                )

        return self
