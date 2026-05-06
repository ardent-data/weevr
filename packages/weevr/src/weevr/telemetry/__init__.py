"""weevr telemetry — structured observability for execution pipelines."""


# Federated namespace: weevr.telemetry splits across two wheels —
# contract types (results, span, trace, events) live in weevr-core
# while runtime helpers (collector, logging) and this package's
# __init__.py stay engine-side. The eager imports below pull in
# weevr-core's contributions, so __path__ must include the core's
# telemetry/ directory before they execute. See weevr/__init__.py for
# the broader rationale; the same helper is mirrored in every
# federated sub-package's __init__.py.
def _extend_namespace_path() -> None:
    """Append sibling weevr.telemetry/ directories on sys.path to ``__path__``."""
    import os.path as osp
    import sys

    components = __name__.split(".")
    src_entries_seen = 0
    candidates_added = 0
    for entry in sys.path:
        if not entry or osp.basename(entry) != "src":
            continue
        src_entries_seen += 1
        candidate = osp.join(entry, *components)
        if osp.isdir(candidate) and candidate not in __path__:
            __path__.append(candidate)
            candidates_added += 1
    if src_entries_seen > 0 and candidates_added == 0:
        import warnings

        warnings.warn(
            f"weevr federated namespace {__name__!r}: no sibling "
            "contributor found on sys.path. If this is an editable "
            "workspace install, run `uv sync --dev` from the repo root "
            "to install both wheels.",
            stacklevel=2,
        )


_extend_namespace_path()
del _extend_namespace_path

from weevr.telemetry.collector import SpanBuilder, SpanCollector  # noqa: E402
from weevr.telemetry.events import LogEvent, create_log_event  # noqa: E402
from weevr.telemetry.logging import (  # noqa: E402
    LogLevel,
    StructuredJsonFormatter,
    configure_logging,
)
from weevr.telemetry.results import (  # noqa: E402
    AssertionResult,
    ExportResult,
    LoomTelemetry,
    ThreadTelemetry,
    ValidationResult,
    WeaveTelemetry,
)
from weevr.telemetry.span import (  # noqa: E402
    ExecutionSpan,
    SpanEvent,
    SpanStatus,
    generate_span_id,
    generate_trace_id,
)
from weevr.telemetry.trace import LoomTrace, ThreadTrace, WeaveTrace  # noqa: E402

__all__ = [
    # Span model
    "ExecutionSpan",
    "SpanEvent",
    "SpanStatus",
    "generate_trace_id",
    "generate_span_id",
    # Collector
    "SpanCollector",
    "SpanBuilder",
    # Events and logging
    "LogEvent",
    "create_log_event",
    "LogLevel",
    "StructuredJsonFormatter",
    "configure_logging",
    # Telemetry results
    "ValidationResult",
    "AssertionResult",
    "ExportResult",
    "ThreadTelemetry",
    "WeaveTelemetry",
    "LoomTelemetry",
    # Trace
    "ThreadTrace",
    "WeaveTrace",
    "LoomTrace",
]
