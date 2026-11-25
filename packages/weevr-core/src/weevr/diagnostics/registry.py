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

from weevr.diagnostics.record import Category, Diagnostic, Severity

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
    """A config-category entry with error severity."""
    return RegistryEntry(code, name, Category.CONFIG, Severity.ERROR, summary, **kwargs)


def _config_warning(code: str, name: str, summary: str, **kwargs: Any) -> RegistryEntry:
    """A config-category entry with warning severity."""
    return RegistryEntry(code, name, Category.CONFIG, Severity.WARNING, summary, **kwargs)


def _platform(code: str, name: str, summary: str, **kwargs: Any) -> RegistryEntry:
    """A platform-category entry with error severity."""
    return RegistryEntry(code, name, Category.PLATFORM, Severity.ERROR, summary, **kwargs)


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
    _config(
        "C103",
        "invalid-value",
        "A value is not allowed: not one of the allowed values, or not in the required form.",
    ),
    _config("C104", "unknown-step-type", "A pipeline or hook step names no known step type."),
    _config("C105", "value-out-of-range", "A value breaks a length, range or pattern limit."),
    _config(
        "C110",
        "unknown-key",
        "A key the schema does not define. Reserved for strict schemas.",
        stability=Stability.EXPERIMENTAL,
    ),
    _config(
        "C111",
        "removed-key",
        "A key this version no longer reads; the hint says why it was removed.",
    ),
    _config(
        "C112",
        "renamed-key",
        "A key this version reads under a new name; expected names the new key.",
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
        "config_version names a major version this release does not read. A 1.x version "
        "is a v1 document; the hint names the weevr-cli migrate command.",
    ),
    _config(
        "C204",
        "unknown-config-kind",
        "The config's kind is not one weevr knows.",
    ),
    # C205–C209 — envelope and project root (config/loader.py, config/project.py, config/root.py)
    _config(
        "C205",
        "name-mismatch",
        "The declared name does not match the file name without its extension.",
    ),
    _config(
        "C206",
        "missing-kind",
        "The document declares no kind. The hint names the weevr-cli migrate command.",
    ),
    _config(
        "C207",
        "kind-extension-mismatch",
        "The declared kind disagrees with the file's typed extension; expected names the kind.",
    ),
    _config(
        "C208",
        "engine-version-unsatisfied",
        "The installed weevr is outside the project's requires_weevr specifier, or its "
        "version cannot be determined (no package metadata); the message names the cause.",
    ),
    _config(
        "C209",
        "project-root-not-found",
        "The Context's project directory holds no weevr.yaml; expected lists every location "
        "probed.",
    ),
    # C210– — project document (config/project.py)
    _config(
        "C210",
        "reserved-project-block",
        "weevr.yaml declares a project-scope block (connections, params, defaults, "
        "audit_templates) that is reserved and not yet supported.",
    ),
    # C3xx — references and entry forms (config/resolver.py, macros.py, warp.py)
    _config(
        "C301",
        "reference-not-found",
        "A ref names a file that does not exist, or a named weave has no file or inline body.",
    ),
    _config("C302", "circular-reference", "A ref leads back to a file already being loaded."),
    _config(
        "C303",
        "reference-outside-project",
        "A ref or weave name resolves to a path outside the project root.",
    ),
    _config("C304", "invalid-reference", "A ref is not a path string."),
    _config("C305", "missing-entry-name", "An inline weave or thread entry has no name."),
    _config(
        "C306",
        "duplicate-entry-name",
        "Two thread entries in a weave have the same effective name.",
    ),
    _config(
        "C307",
        "missing-entry-alias",
        "A thread ref appears more than once in a weave without 'as' aliases.",
    ),
    _config(
        "C308",
        "invalid-foreach",
        "A foreach block is missing a required part or has a part of the wrong shape.",
    ),
    _config(
        "C309",
        "foreach-not-supported",
        "A foreach block appears where steps are not expanded (inside a 'with' sub-pipeline).",
    ),
    _config("C310", "warp-not-found", "A named warp file is not in any searched location."),
    _config(
        "C311",
        "absolute-path-reference",
        "A reference is an absolute path or URI where a path relative to the project belongs.",
    ),
    _config(
        "C312",
        "unsupported-reference-scheme",
        "A reference names a scheme (scheme://) where a project-relative path belongs.",
    ),
    _config(
        "C313",
        "name-only-entry",
        "A weave or thread entry gives a name but carries neither ref: nor an inline body.",
    ),
    # C350–C359 — loom entries
    _config(
        "C350",
        "duplicate-weave-name",
        "Two weave entries in a loom resolve to the same name.",
    ),
    # C4xx — cross-field rules
    _config(
        "C400",
        "invalid-config",
        "A config rule was broken, in a way no more specific code covers.",
    ),
    # C401–C439 — rule families shared by every block, then pipeline step rules
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
        "An option required by the block's kind, mode or other options is not set.",
    ),
    _config(
        "C404",
        "option-not-applicable",
        "An option is set that the block's kind or mode does not use.",
    ),
    _config("C405", "values-must-differ", "Two options must hold different values but are equal."),
    _config(
        "C406",
        "value-out-of-order",
        "A range's start comes after its end, or equals it where the rule needs a strict order.",
    ),
    # C440–C479 — other model rules (model/*.py)
    _config(
        "C440",
        "column-in-several-groups",
        "A column is listed in more than one group where each column may belong to only one.",
    ),
    _config(
        "C441",
        "incremental-dedup-order",
        "A deduplication on an incremental read does not keep the latest row by the watermark.",
    ),
    _config(
        "C442",
        "name-collision",
        "A name or alias collides with another name in the same scope.",
    ),
    _config(
        "C443",
        "forward-reference",
        "A name is used before it is declared.",
    ),
    _config(
        "C444",
        "duplicate-column",
        "A column name is declared more than once.",
    ),
    _config(
        "C445",
        "undeclared-column-reference",
        "A key or reference names a column that is not declared.",
    ),
    # C480–C499 — load-time cross-field checks (config/validation.py)
    _config(
        "C480",
        "cdc-requires-merge",
        "A thread in cdc load mode does not write with mode 'merge'.",
    ),
    _config(
        "C481",
        "cdc-requires-watermark-column",
        "A cdc mapping with update or delete values has no load.watermark_column to order "
        "same-key changes.",
    ),
    _config(
        "C482",
        "incremental-watermark-overwrite",
        "A thread in incremental_watermark load mode would overwrite its target with each "
        "incremental slice.",
    ),
    _config_warning(
        "C483",
        "incremental-parameter-without-param",
        "A thread in incremental_parameter load mode references no ${param.*} variable in "
        "its steps or sources.",
    ),
    _config_warning(
        "C484",
        "watermark-inclusive-append",
        "An inclusive watermark with write mode 'append' can duplicate rows on re-reads.",
    ),
    _config(
        "C485",
        "dimension-derived-write-field",
        "A thread with a dimension target sets a write field the engine derives itself.",
    ),
    _config(
        "C486",
        "dimension-write-mode",
        "A thread with a dimension target sets write.mode, but dimension targets always merge.",
    ),
    _config(
        "C487",
        "dimension-keys-field",
        "A thread with a dimension target sets a keys field that belongs in the dimension block.",
    ),
    _config_warning(
        "C490",
        "scope-defaults-execution-ignored",
        "A loom or weave declares defaults.execution, which thread-scoped execution does "
        "not apply.",
    ),
    _config_warning(
        "C491",
        "thread-execution-ignored",
        "A thread declares an execution block, which is not applied at thread scope.",
    ),
    _config_warning(
        "C492",
        "weave-trace-ineffective",
        "A weave sets trace: true under a loom whose effective trace is false.",
    ),
    # P101–P149 — variables and parameters (config/resolver.py)
    _config(
        "P101",
        "missing-required-parameter",
        "A required parameter has no runtime value and no default.",
    ),
    _config(
        "P102",
        "parameter-type-mismatch",
        "A parameter value is not of the type its declaration names.",
    ),
    _config(
        "P103",
        "invalid-parameter-format",
        "A date or timestamp parameter is not in the expected ISO format.",
    ),
    _config(
        "P110",
        "unresolved-variable",
        "A ${...} variable has no value and no default.",
    ),
    _config(
        "P111",
        "bare-variable-reference",
        "A ${...} reference names no namespace: ${x} instead of ${param.x}.",
    ),
    _config(
        "P112",
        "unknown-variable-namespace",
        "A ${...} reference starts with an unknown namespace, or names an unknown key "
        "of a thread, weave or loom namespace.",
    ),
    _config_warning(
        "P120",
        "undeclared-parameter",
        "A ${param.x} reference is bound, but no params: declaration in its scope chain "
        "covers it. An error under strict_params.",
    ),
    _config_warning(
        "P121",
        "unused-supplied-parameter",
        "A runtime parameter is supplied that nothing in the loaded tree declares or "
        "references. An error under strict_params.",
    ),
    _config_warning(
        "P123",
        "unconsumed-pin",
        "A weave or thread entry pins a parameter that nothing beneath the entry reads "
        "or declares.",
    ),
    # R101–R119 — lookup and column-set definitions (model/*.py)
    _config(
        "R101",
        "lookup-key-values-overlap",
        "A lookup's key and value columns overlap.",
    ),
    # R120–R139 — lookup and column-set references checked at load
    _config(
        "R120",
        "undefined-resolve-lookup",
        "A resolve step names a lookup that no visible scope defines.",
    ),
    _config_warning(
        "R121",
        "undefined-resolve-lookup-ignored",
        "A resolve step with on_failure: warn names a lookup that no visible scope defines.",
    ),
    _config(
        "R122",
        "undefined-source-lookup",
        "A lookup-bound source names a lookup that no visible scope defines.",
    ),
    _config_warning(
        "R123",
        "source-lookup-outside-thread",
        "A thread loaded on its own has a lookup-bound source whose lookup it does not define.",
    ),
    _config(
        "R124",
        "undefined-column-set",
        "A rename step names a column set that no visible scope defines.",
    ),
    _config_warning(
        "R125",
        "column-set-outside-thread",
        "A thread loaded on its own has a rename step whose column set it does not define.",
    ),
    # R140–R149 — audit templates (config/audit_templates.py)
    _config(
        "R140",
        "unknown-audit-template",
        "An audit template name is neither user-defined nor a built-in preset.",
    ),
    # R2xx — connections
    _config(
        "R201", "undefined-connection", "A source or target names a connection no scope declares."
    ),
    # D1xx — plan and dependencies
    _config(
        "D100",
        "plan-failed",
        "A weave's plan could not be built, in a way no more specific code covers.",
    ),
    _config(
        "D101",
        "circular-dependency",
        "The threads of a weave depend on each other in a cycle.",
    ),
    _config(
        "D102",
        "conflicting-target-writers",
        "Several threads write one target without being ordered or condition-gated.",
    ),
    _config(
        "D103",
        "undefined-dependency",
        "A thread declares a dependency on a thread the weave does not define.",
    ),
    _config("D104", "weave-not-found", "A loom lists a weave the loaded config does not hold."),
    # S1xx — source existence
    # S101–S119 — sources checked in validate mode (context.py)
    _platform("S101", "source-not-found", "A source does not exist at its resolved location."),
    _platform(
        "S102",
        "source-probe-failed",
        "A source's existence could not be checked, because probing its location failed.",
    ),
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
