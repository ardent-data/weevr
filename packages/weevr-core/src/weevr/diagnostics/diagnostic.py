"""The ``Diagnostic`` record: one problem weevr reports, identified by a stable code."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, fields, replace
from enum import StrEnum
from typing import Any


class Severity(StrEnum):
    """How serious a diagnostic is."""

    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


class Category(StrEnum):
    """What a diagnostic is about: the config, the data, or the platform."""

    CONFIG = "config"
    DATA = "data"
    PLATFORM = "platform"


@dataclass(frozen=True)
class Diagnostic:
    """One problem weevr reports.

    ``code`` is the contract: it never changes meaning and is never reused.
    ``message`` is human text and may be reworded in any release.

    Attributes:
        code: Stable code, one category letter and three digits (``C101``).
        name: Kebab-case label for the code (``missing-field``).
        severity: Error, warning, or info.
        category: Config, data, or platform.
        message: Human-readable description.
        file: The authored file the problem is in, as a path or URI.
        pointer: JSON pointer into ``file``'s document (``""`` is the root).
        line: 1-based line in ``file``, when known.
        column: 1-based column in ``file``, when known.
        resolved_path: JSON pointer into the resolved document, after
            variables, references and the cascade.
        location_fallback: ``True`` when the value came from elsewhere (a
            cascade or a parameter) and ``file``/``pointer`` name the config
            that received it rather than the one that declared it.
        node: Path of the loom, weave and thread the problem belongs to.
        step_index: Index of the pipeline step, when the problem is in one.
        step_type: Type of that step.
        expected: Allowed keys or values, when known.
        hint: What to change.
        docs_url: The code's entry in the published code list.
    """

    code: str
    name: str
    severity: Severity
    category: Category
    message: str
    file: str | None = None
    pointer: str | None = None
    line: int | None = None
    column: int | None = None
    resolved_path: str | None = None
    location_fallback: bool = False
    node: str | None = None
    step_index: int | None = None
    step_type: str | None = None
    expected: tuple[str, ...] | None = None
    hint: str | None = None
    docs_url: str | None = None

    def __post_init__(self) -> None:
        """Normalize enums and freeze ``expected`` into a tuple of strings."""
        object.__setattr__(self, "severity", Severity(self.severity))
        object.__setattr__(self, "category", Category(self.category))
        if self.expected is not None and not isinstance(self.expected, tuple):
            object.__setattr__(self, "expected", _as_tuple(self.expected))

    @property
    def is_error(self) -> bool:
        """Whether this diagnostic has error severity."""
        return self.severity is Severity.ERROR

    def located(self, file: str) -> Diagnostic:
        """Return this diagnostic with ``file`` set, unless it already names one."""
        return self if self.file is not None else replace(self, file=file)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-ready dict with every field, ``None`` where unknown."""
        data: dict[str, Any] = {f.name: getattr(self, f.name) for f in fields(self)}
        data["severity"] = self.severity.value
        data["category"] = self.category.value
        if self.expected is not None:
            data["expected"] = list(self.expected)
        return data

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> Diagnostic:
        """Build a diagnostic from :meth:`to_dict` output.

        Args:
            data: A dict with at least ``code``, ``name``, ``severity``,
                ``category`` and ``message``. Unknown keys are ignored.

        Returns:
            The diagnostic.
        """
        known = {f.name for f in fields(cls)}
        values = {key: value for key, value in data.items() if key in known}
        if values.get("expected") is not None:
            values["expected"] = _as_tuple(values["expected"])
        return cls(**values)


def _as_tuple(values: Iterable[Any]) -> tuple[str, ...]:
    return tuple(str(value) for value in values)
