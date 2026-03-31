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
        connection: Named connection reference (delta type only). Mutually
            exclusive with ``path`` and ``alias``. Requires ``table``.
        schema_override: Schema override within the connection's lakehouse.
            Aliased as ``schema`` in YAML configs.
        table: Table name within the connection's lakehouse. Required when
            ``connection`` is set; invalid without it.
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
    connection: str | None = Field(
        default=None,
        description="Reference to a named connection defined at thread, weave, or loom level.",
    )
    schema_override: str | None = Field(
        default=None,
        alias="schema",
        description="Schema override within the connection's lakehouse.",
    )

    model_config = {"frozen": True, "populate_by_name": True}
    table: str | None = Field(
        default=None,
        description="Table name within the connection's lakehouse.",
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

        - ``alias`` and ``connection`` require ``type == "delta"``.
        - ``connection`` and ``alias`` are mutually exclusive.
        - ``connection`` and ``path`` are mutually exclusive.
        - ``connection`` requires ``table``; ``table`` requires ``connection``.
        - When enabled, exactly one of ``path``, ``alias``, or
          ``connection`` + ``table`` must be set.
        - ``name`` must be a valid Python identifier.
        """
        if not self.name.isidentifier():
            raise ValueError(f"Export name '{self.name}' is not a valid identifier")

        if self.alias is not None and self.type != "delta":
            raise ValueError(
                f"Export '{self.name}': alias is only valid for delta type, got type='{self.type}'"
            )

        if self.connection is not None and self.type != "delta":
            raise ValueError(
                f"Export '{self.name}': connection is only valid for delta type, "
                f"got type='{self.type}'"
            )

        if self.connection is not None and self.alias is not None:
            raise ValueError(f"Export '{self.name}': connection and alias are mutually exclusive")

        if self.connection is not None and self.path is not None:
            raise ValueError(f"Export '{self.name}': connection and path are mutually exclusive")

        if self.connection is not None and not self.table:
            raise ValueError(f"Export '{self.name}': connection requires table to be set")

        if self.table is not None and not self.connection:
            raise ValueError(f"Export '{self.name}': table requires connection to be set")

        if self.enabled:
            has_path = bool(self.path)
            has_alias = bool(self.alias)
            has_connection = bool(self.connection)
            destinations = sum([has_path, has_alias, has_connection])
            if destinations != 1:
                raise ValueError(
                    f"Export '{self.name}': exactly one of 'path', 'alias', or 'connection' "
                    f"must be set (got path={has_path}, alias={has_alias}, "
                    f"connection={has_connection})"
                )

        return self
