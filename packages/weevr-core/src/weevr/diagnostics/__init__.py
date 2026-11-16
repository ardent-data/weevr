"""Coded diagnostics: one record shape and one code registry for every problem weevr reports.

Match on :attr:`Diagnostic.code`, never on message text. The published code
list is generated from :data:`REGISTRY`.
"""

from weevr.diagnostics.diagnostic import Category, Diagnostic, Severity
from weevr.diagnostics.registry import (
    DOCS_BASE_URL,
    REGISTRY,
    RETIRED_CODES,
    RegistryEntry,
    Stability,
    diagnostic,
    track_usage,
)

__all__ = [
    "DOCS_BASE_URL",
    "REGISTRY",
    "RETIRED_CODES",
    "Category",
    "Diagnostic",
    "RegistryEntry",
    "Severity",
    "Stability",
    "diagnostic",
    "track_usage",
]
