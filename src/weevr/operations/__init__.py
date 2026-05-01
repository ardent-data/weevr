"""weevr operations — readers, transforms, hashing, mapping, writers, exports, validation."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from weevr.operations.assertions import evaluate_assertions
    from weevr.operations.audit import (
        AuditContext,
        build_sources_json,
        inject_audit_columns,
        resolve_audit_columns,
    )
    from weevr.operations.exports import (
        resolve_export_path,
        resolve_exports,
        write_export,
    )
    from weevr.operations.hashing import compute_keys
    from weevr.operations.pipeline import run_pipeline
    from weevr.operations.quarantine import write_quarantine
    from weevr.operations.readers import read_source, read_sources
    from weevr.operations.validation import ValidationOutcome, validate_dataframe
    from weevr.operations.writers import apply_target_mapping, write_target

__all__ = [
    "AuditContext",
    "apply_target_mapping",
    "build_sources_json",
    "compute_keys",
    "evaluate_assertions",
    "inject_audit_columns",
    "read_source",
    "read_sources",
    "resolve_audit_columns",
    "resolve_export_path",
    "resolve_exports",
    "run_pipeline",
    "validate_dataframe",
    "ValidationOutcome",
    "write_export",
    "write_quarantine",
    "write_target",
]

# Map each public attribute to the submodule that defines it. Lazy access
# preserves every existing import path (``from weevr.operations import X``)
# while letting submodule paths (``from weevr.operations.reserved_words import
# resolve_effective_words``) resolve without dragging the Spark-bound
# transforms / readers / writers / pipeline modules.
_LAZY_ATTRS: dict[str, str] = {
    "AuditContext": "weevr.operations.audit",
    "build_sources_json": "weevr.operations.audit",
    "inject_audit_columns": "weevr.operations.audit",
    "resolve_audit_columns": "weevr.operations.audit",
    "evaluate_assertions": "weevr.operations.assertions",
    "resolve_export_path": "weevr.operations.exports",
    "resolve_exports": "weevr.operations.exports",
    "write_export": "weevr.operations.exports",
    "compute_keys": "weevr.operations.hashing",
    "run_pipeline": "weevr.operations.pipeline",
    "write_quarantine": "weevr.operations.quarantine",
    "read_source": "weevr.operations.readers",
    "read_sources": "weevr.operations.readers",
    "ValidationOutcome": "weevr.operations.validation",
    "validate_dataframe": "weevr.operations.validation",
    "apply_target_mapping": "weevr.operations.writers",
    "write_target": "weevr.operations.writers",
}


def __getattr__(name: str) -> object:
    module_path = _LAZY_ATTRS.get(name)
    if module_path is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    import importlib

    module = importlib.import_module(module_path)
    return getattr(module, name)
