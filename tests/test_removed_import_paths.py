"""Locks for import paths removed in 2.0.

The core/engine wheel split left back-compat re-exports at old import
paths. 2.0 removes them; each case here fails if a removed path or name
comes back. The comment above each case names the canonical home.
"""

from __future__ import annotations

import importlib

import pytest

REMOVED_MODULES = [
    # parse_duration -> weevr.duration
    "weevr.config.duration",
    # reserved-word presets and resolve_effective_words -> weevr.reserved_words
    "weevr.operations.reserved_words",
]


@pytest.mark.parametrize("module", REMOVED_MODULES)
def test_removed_module_does_not_import(module: str) -> None:
    """Each removed back-compat module no longer exists."""
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module(module)


# Audit-template primitives -> weevr.config.audit_templates. The engine-side
# weevr.operations.audit keeps only its Spark-bound functions.
AUDIT_REMOVED_NAMES = [
    "BUILTIN_AUDIT_PRESETS",
    "CONTEXT_VAR_PATTERN",
    "CONTEXT_VARIABLE_FIELDS",
    "HIERARCHY_CONTEXT_KEYS",
    "resolve_audit_columns",
    "apply_audit_exclusions",
    "resolve_template_columns",
    "merge_audit_columns_with_templates",
]

# Still importable as incidental module attributes (operations.audit uses
# them itself), but no longer exported.
AUDIT_UNEXPORTED_NAMES = ["AuditContext", "resolve_context_variables"]


@pytest.mark.parametrize("name", AUDIT_REMOVED_NAMES)
def test_operations_audit_no_longer_reexports(name: str) -> None:
    """``from weevr.operations.audit import <name>`` fails for each removed re-export."""
    audit = importlib.import_module("weevr.operations.audit")
    assert not hasattr(audit, name)


@pytest.mark.parametrize("name", AUDIT_UNEXPORTED_NAMES)
def test_operations_audit_does_not_export_core_names(name: str) -> None:
    """Core names the module uses internally are not part of its exports."""
    audit = importlib.import_module("weevr.operations.audit")
    assert name not in audit.__all__


def test_operations_audit_exports_only_engine_functions() -> None:
    """``weevr.operations.audit`` exports exactly its Spark-bound functions."""
    audit = importlib.import_module("weevr.operations.audit")
    assert sorted(audit.__all__) == ["build_sources_json", "inject_audit_columns"]


# weevr.operations (package) -> import these from weevr.config.audit_templates.
@pytest.mark.parametrize("name", ["AuditContext", "resolve_audit_columns"])
def test_operations_package_no_longer_reexports_audit_templates(name: str) -> None:
    """The ``weevr.operations`` package does not re-export core audit-template names."""
    operations = importlib.import_module("weevr.operations")
    assert name not in operations.__all__
    with pytest.raises(AttributeError):
        getattr(operations, name)
