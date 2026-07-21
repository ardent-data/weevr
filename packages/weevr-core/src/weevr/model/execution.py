"""Execution configuration model."""

from enum import StrEnum

from pydantic import Field

from weevr.model.base import FrozenBase


class LogLevel(StrEnum):
    """Configurable log level for weevr execution.

    Controls the verbosity of structured logging output during pipeline
    execution. Maps to Python logging levels internally.
    """

    MINIMAL = "minimal"
    STANDARD = "standard"
    VERBOSE = "verbose"
    DEBUG = "debug"


class ExecutionConfig(FrozenBase):
    """Runtime execution settings declared on the top-level ``execution:`` block.

    Canonical declaration sites are the top-level ``execution:`` field on
    Loom and Weave. The effective settings for a weave execution merge
    field-level (see :func:`resolve_effective_execution`) rather than
    replacing whole-block. Thread-scoped execution settings are declared
    but not applied in v1.x.

    Attributes:
        log_level: Logging verbosity for execution output.
        trace: Whether to collect execution spans for telemetry.
        max_parallel_threads: Upper bound on concurrently executing
            threads within a weave's parallel group. Unset means
            unbounded (pool sized to the group).
        capture_samples: Whether output and quarantine samples are
            captured at write time. Off by default; preview always
            samples.
    """

    log_level: LogLevel = Field(
        default=LogLevel.STANDARD,
        description="Logging verbosity for execution output.",
    )
    trace: bool = Field(
        default=True,
        description="Whether to collect execution spans for telemetry.",
    )
    max_parallel_threads: int | None = Field(
        default=None,
        ge=1,
        description=(
            "Maximum threads executing concurrently within a weave's "
            "parallel group. Unset means unbounded."
        ),
    )
    capture_samples: bool = Field(
        default=False,
        description=(
            "Whether to capture 10-row output and quarantine samples at "
            "write time for display. Preview mode always captures samples "
            "regardless of this setting."
        ),
    )


def resolve_effective_execution(
    parent: ExecutionConfig | None,
    child: ExecutionConfig | None,
) -> ExecutionConfig:
    """Compute the effective execution settings for a child scope.

    Merges field-level: a field the child explicitly set wins, otherwise
    a field the parent explicitly set, otherwise the field default.
    Explicitness is read from Pydantic's ``model_fields_set`` — required
    because ``log_level`` and ``trace`` have non-None defaults, so
    explicit-vs-default is only knowable on hydrated instances.

    Args:
        parent: The enclosing scope's ``execution:`` block (e.g. Loom),
            or None when absent.
        child: The narrower scope's ``execution:`` block (e.g. Weave),
            or None when absent.

    Returns:
        A new ExecutionConfig holding the effective field values.
    """
    values: dict[str, object] = {}
    for source in (parent, child):
        if source is None:
            continue
        for field_name in source.model_fields_set:
            values[field_name] = getattr(source, field_name)
    return ExecutionConfig(**values)  # type: ignore[arg-type]
