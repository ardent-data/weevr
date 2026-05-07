"""Lazy-init invariants for the two PEP 562 weevr packages.

Phase A introduced module-level ``__getattr__`` lookups in
``weevr/__init__.py`` and ``weevr/operations/__init__.py`` so that
importing ``weevr`` (or ``weevr.operations``) does not pull in PySpark.
Both modules carry a ``_LAZY_ATTRS`` map that pins each public attribute
to the submodule that supplies it. ``__all__`` lists the same attributes
as part of the public surface.

If the two drift apart, two failure modes follow: an entry in ``__all__``
without a ``_LAZY_ATTRS`` row resolves to ``AttributeError`` at runtime,
and a row in ``_LAZY_ATTRS`` without an ``__all__`` entry shows up in
``dir()`` but not in static autocomplete. Either is a regression.

The Phase B federated split (engine + weevr-core) does not adopt
``_LAZY_ATTRS`` for ``weevr.engine`` or ``weevr.telemetry`` — those
``__init__.py`` files use eager imports because they live in the engine
wheel, where PySpark is already loaded. The invariant therefore covers
exactly the two PEP 562 modules.
"""

from __future__ import annotations

import importlib

import pytest

LAZY_MODULES = ["weevr", "weevr.operations"]


@pytest.mark.parametrize("module_name", LAZY_MODULES)
def test_lazy_attrs_match_all(module_name: str) -> None:
    """``set(_LAZY_ATTRS) == set(__all__)`` for every PEP 562 weevr module."""
    module = importlib.import_module(module_name)
    lazy = set(getattr(module, "_LAZY_ATTRS", {}))
    public = set(module.__all__)
    assert lazy == public, (
        f"{module_name}: _LAZY_ATTRS and __all__ disagree.\n"
        f"  in __all__ but not _LAZY_ATTRS: {public - lazy}\n"
        f"  in _LAZY_ATTRS but not __all__: {lazy - public}"
    )


@pytest.mark.parametrize("module_name", LAZY_MODULES)
def test_lazy_attrs_resolve(module_name: str) -> None:
    """Every ``_LAZY_ATTRS`` entry actually resolves through ``__getattr__``."""
    module = importlib.import_module(module_name)
    lazy: dict[str, str] = module._LAZY_ATTRS
    for name, target_module in lazy.items():
        attr = getattr(module, name)
        target = importlib.import_module(target_module)
        assert getattr(target, name) is attr, (
            f"{module_name}.{name} did not resolve to {target_module}.{name}"
        )
