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
]


@pytest.mark.parametrize("module", REMOVED_MODULES)
def test_removed_module_does_not_import(module: str) -> None:
    """Each removed back-compat module no longer exists."""
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module(module)
