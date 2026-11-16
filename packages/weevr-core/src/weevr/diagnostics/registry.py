"""The code registry: every code weevr reports, and the factory that builds diagnostics.

Codes are one category letter and three digits, grouped in blocks:

- ``C0xx`` file read and YAML parse; ``C1xx`` schema shape; ``C2xx`` envelope,
  config kind and ``config_version``; ``C3xx`` references and entry forms;
  ``C4xx`` cross-field rules.
- ``P1xx`` variables and parameters.
- ``R1xx`` registries (lookups, column sets); ``R2xx`` connections.
- ``D1xx`` DAG and plan.
- ``S1xx`` source and target existence; ``S2xx`` Delta storage.
- ``E1xx`` execution.
- ``Q1xx`` quality gates; ``Q2xx`` validation rules.

A code never changes meaning and is never reused. A code that is no longer
emitted is marked deprecated, then moved to :data:`RETIRED_CODES`.
"""

from __future__ import annotations

import re
import sys
from collections import Counter
from collections.abc import Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from enum import StrEnum
from types import MappingProxyType
from typing import Any

from weevr.diagnostics.diagnostic import Category, Diagnostic, Severity

DOCS_BASE_URL = "https://ardent-data.github.io/weevr/latest/reference/diagnostics/"
"""The published code list; each code's entry is at ``#<code in lowercase>``."""

_CODE = re.compile(r"^[A-Z]\d{3}$")


class Stability(StrEnum):
    """Whether a code may still change."""

    STABLE = "stable"
    EXPERIMENTAL = "experimental"
    DEPRECATED = "deprecated"


@dataclass(frozen=True)
class RegistryEntry:
    """One registered code.

    Attributes:
        code: The code (``C101``).
        name: Kebab-case label; may be sharpened in a minor release.
        category: Config, data, or platform.
        severity: Default severity of diagnostics with this code.
        summary: One line saying what the code means.
        stability: Stable, experimental, or deprecated.
        removed_in: The version a deprecated code stops being emitted in.
    """

    code: str
    name: str
    category: Category
    severity: Severity
    summary: str
    stability: Stability = Stability.STABLE
    removed_in: str | None = None

    @property
    def docs_anchor(self) -> str:
        """The code's anchor in the published code list."""
        return self.code.lower()

    @property
    def docs_url(self) -> str:
        """The code's entry in the published code list."""
        return f"{DOCS_BASE_URL}#{self.docs_anchor}"


def _config(code: str, name: str, summary: str, **kwargs: Any) -> RegistryEntry:
    return RegistryEntry(code, name, Category.CONFIG, Severity.ERROR, summary, **kwargs)


_ENTRIES: tuple[RegistryEntry, ...] = (
    # C0xx — reading and parsing a config file
    _config("C001", "config-file-not-found", "The config file does not exist."),
    _config("C002", "config-file-unreadable", "The config file exists but could not be read."),
    _config("C003", "config-file-not-utf8", "The config file is not valid UTF-8 text."),
    _config("C004", "empty-config", "The config file is empty or holds only null."),
    _config("C005", "yaml-syntax-error", "The config file is not valid YAML."),
    _config("C006", "config-not-a-mapping", "The top level of the config is not a mapping."),
    _config(
        "C007",
        "unsupported-extension",
        "The file extension is not one weevr loads (.thread, .weave, .loom, .warp, .yaml, .yml).",
    ),
    # C1xx — schema shape
    _config(
        "C100",
        "schema-violation",
        "The config does not match the schema, in a way no more specific code covers.",
    ),
    _config("C101", "missing-field", "A required key is missing."),
    _config(
        "C102",
        "wrong-type",
        "A value has the wrong type: one code for every type (string, number, boolean, "
        "list, mapping), and for a value no member of a union accepts.",
    ),
    _config("C103", "invalid-value", "A value is not one of the allowed values."),
    _config("C104", "unknown-step-type", "A pipeline or hook step names no known step type."),
    _config("C105", "value-out-of-range", "A value breaks a length, range or pattern limit."),
    _config(
        "C110",
        "unknown-key",
        "A key the schema does not define. Reserved for strict schemas.",
        stability=Stability.EXPERIMENTAL,
    ),
    # C2xx — envelope, config kind and config_version
    _config("C201", "missing-config-version", "The config has no config_version."),
    _config(
        "C202",
        "invalid-config-version",
        "config_version is not a 'major.minor' string of integers.",
    ),
    _config(
        "C203",
        "unsupported-config-version",
        "config_version names a major version this release does not read.",
    ),
    _config(
        "C204",
        "unknown-config-kind",
        "The config's kind could not be determined, or is not one weevr knows.",
    ),
    # C3xx — references and entry forms (config/resolver.py, macros.py, locations.py)
    # C4xx — cross-field rules
    _config(
        "C400",
        "invalid-config",
        "A config rule was broken, in a way no more specific code covers.",
    ),
    # C401–C439 — pipeline step rules (model/pipeline.py, model/sub_pipeline.py)
    _config(
        "C401", "empty-collection", "A list, mapping or pattern that must have content is empty."
    ),
    _config(
        "C402",
        "conflicting-options",
        "Two or more options that exclude each other are set together.",
    ),
    _config(
        "C403",
        "missing-required-option",
        "An option the step needs for its mode or its other options is not set.",
    ),
    _config(
        "C404",
        "option-not-applicable",
        "An option is set that the step's chosen mode does not use.",
    ),
    _config("C405", "values-must-differ", "Two options must hold different values but are equal."),
    _config("C406", "value-out-of-order", "A lower bound is greater than its upper bound."),
    _config("C407", "invalid-format-pattern", "A format pattern contains characters it may not."),
    _config(
        "C408", "missing-column-placeholder", "An expression lacks its required {col} placeholder."
    ),
    # C440–C479 — other model rules (model/*.py)
    # C480–C499 — load-time cross-field checks (config/validation.py)
    # P1xx — variables and parameters
    # R101–R119 — lookup and column-set definitions (model/*.py)
    # R120–R139 — lookup and column-set references checked at load
    # D1xx — plan and dependencies
    # S1xx — source existence
)

RETIRED_CODES: frozenset[str] = frozenset()
"""Codes no longer in use. They are never registered again."""


def build_registry(
    entries: Iterable[RegistryEntry], retired: frozenset[str]
) -> Mapping[str, RegistryEntry]:
    """Index entries by code, enforcing the registry's invariants.

    Args:
        entries: The registered codes.
        retired: Codes that may never be registered again.

    Returns:
        A read-only mapping of code to entry, in registration order.

    Raises:
        ValueError: If a code is malformed, a code or name repeats, or a
            retired code is registered.
    """
    by_code: dict[str, RegistryEntry] = {}
    names: set[str] = set()
    for entry in entries:
        if not _CODE.match(entry.code):
            raise ValueError(f"Malformed diagnostic code '{entry.code}'")
        if entry.code in retired:
            raise ValueError(f"Diagnostic code '{entry.code}' is retired and cannot be reused")
        if entry.code in by_code:
            raise ValueError(f"Diagnostic code '{entry.code}' is registered twice")
        if entry.name in names:
            raise ValueError(f"Diagnostic name '{entry.name}' is registered twice")
        by_code[entry.code] = entry
        names.add(entry.name)
    return MappingProxyType(by_code)


REGISTRY: Mapping[str, RegistryEntry] = build_registry(_ENTRIES, RETIRED_CODES)
"""Every registered code, by code."""


_usage: Counter[tuple[str, str]] | None = None


@contextmanager
def track_usage() -> Iterator[Counter[tuple[str, str]]]:
    """Count the diagnostics built inside the block, by code and emitting module.

    Tests use it to prove a module emits a specific code rather than a
    generic one. The emitting module is the factory's caller unless the
    caller passes ``origin``.

    Yields:
        A counter keyed by ``(code, module name)``; it fills as the block runs.
    """
    global _usage
    previous = _usage
    counter: Counter[tuple[str, str]] = Counter()
    _usage = counter
    try:
        yield counter
    finally:
        _usage = previous


def diagnostic(
    code: str,
    message: str,
    *,
    severity: Severity | None = None,
    file: str | None = None,
    pointer: str | None = None,
    line: int | None = None,
    column: int | None = None,
    resolved_path: str | None = None,
    location_fallback: bool = False,
    node: str | None = None,
    step_index: int | None = None,
    step_type: str | None = None,
    expected: Sequence[Any] | None = None,
    hint: str | None = None,
    origin: str | None = None,
) -> Diagnostic:
    """Build a diagnostic for a registered code.

    Name, category, default severity and docs URL come from the registry.

    Args:
        code: A registered code.
        message: Human-readable description.
        severity: Overrides the code's default severity.
        file: The authored file.
        pointer: JSON pointer into ``file``'s document.
        line: 1-based line in ``file``.
        column: 1-based column in ``file``.
        resolved_path: JSON pointer into the resolved document.
        location_fallback: ``file``/``pointer`` name the receiving config,
            not the declaring one.
        node: Loom/weave/thread path.
        step_index: Pipeline step index.
        step_type: Pipeline step type.
        expected: Allowed keys or values.
        hint: What to change.
        origin: Module to credit in :func:`track_usage`; defaults to the caller.

    Returns:
        The diagnostic.

    Raises:
        ValueError: If ``code`` is not registered.
    """
    entry = REGISTRY.get(code)
    if entry is None:
        raise ValueError(f"Diagnostic code '{code}' is not registered")
    if _usage is not None:
        module = origin or sys._getframe(1).f_globals.get("__name__", "?")
        _usage[(code, module)] += 1
    return Diagnostic(
        code=code,
        name=entry.name,
        severity=severity or entry.severity,
        category=entry.category,
        message=message,
        file=file,
        pointer=pointer,
        line=line,
        column=column,
        resolved_path=resolved_path,
        location_fallback=location_fallback,
        node=node,
        step_index=step_index,
        step_type=step_type,
        expected=tuple(str(value) for value in expected) if expected is not None else None,
        hint=hint,
        docs_url=entry.docs_url,
    )
