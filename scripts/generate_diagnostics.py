#!/usr/bin/env python3
"""Generate the published diagnostic code list from the weevr code registry.

Writes ``docs/reference/diagnostics.md`` (the page users look codes up in) and
``docs/schema/diagnostics.json`` (the machine-readable list).

Usage:
    python scripts/generate_diagnostics.py          # Generate both files
    python scripts/generate_diagnostics.py --check  # Verify both are up-to-date
"""

from __future__ import annotations

import json
import sys
import textwrap
from pathlib import Path

from weevr.diagnostics import REGISTRY, RETIRED_CODES, RegistryEntry, Stability
from weevr.diagnostics.removed_keys import REMOVED_KEYS, RemovedKey

ROOT = Path(__file__).resolve().parent.parent
PAGE = ROOT / "docs" / "reference" / "diagnostics.md"
JSON_LIST = ROOT / "docs" / "schema" / "diagnostics.json"

FORMAT_VERSION = 1

BLOCKS: dict[str, str] = {
    "C0": "Reading and parsing",
    "C1": "Schema shape",
    "C2": "Envelope, kind and version",
    "C3": "References and entry forms",
    "C4": "Config rules",
    "P1": "Variables and parameters",
    "R1": "Lookups and column sets",
    "R2": "Connections",
    "D1": "Plan and dependencies",
    "S1": "Source and target existence",
    "S2": "Delta storage",
    "E1": "Execution",
    "Q1": "Quality gates",
    "Q2": "Validation rules",
}

INTRO = """\
# Diagnostic Codes

Every problem weevr reports carries a stable code: one letter for the
area and three digits. Match on the code, never on the message. Messages
are written for people and may be reworded in any release; a code keeps
its meaning and is never reused.

Each code also has a kebab-case name that reads well in logs. Names may
be sharpened in a minor release; the code does not change.

| Letter | Area |
|--------|------|
| `C` | Config files: parsing, schema, envelope, references, rules |
| `P` | Variables and parameters |
| `R` | Registries: lookups, column sets, connections |
| `D` | Plan and dependencies |
| `S` | Sources, targets and storage |
| `E` | Execution |
| `Q` | Data quality |

Stability: **stable** codes keep their meaning; **experimental** codes
may change before they are marked stable; **deprecated** codes are no
longer emitted from the version shown.

The same list is available as JSON in
[`diagnostics.json`](https://github.com/ardent-data/weevr/blob/main/docs/schema/diagnostics.json).

!!! note "Generated page"

    This page is generated from the code registry by
    `scripts/generate_diagnostics.py`. Do not edit it by hand.
"""


def _entry_json(entry: RegistryEntry) -> dict[str, object]:
    return {
        "code": entry.code,
        "name": entry.name,
        "category": entry.category.value,
        "severity": entry.severity.value,
        "stability": entry.stability.value,
        "removed_in": entry.removed_in,
        "summary": entry.summary,
        "docs_anchor": entry.docs_anchor,
    }


def render_json() -> str:
    """Render the machine-readable code list."""
    data = {
        "format_version": FORMAT_VERSION,
        "codes": [_entry_json(entry) for entry in sorted(REGISTRY.values(), key=_sort_key)],
        "retired": sorted(RETIRED_CODES),
        "removed_keys": [_removed_json(row) for row in REMOVED_KEYS],
    }
    return json.dumps(data, indent=2) + "\n"


def _removed_json(row: RemovedKey) -> dict[str, object]:
    return {
        "kind": row.kind,
        "path": row.path,
        "replacement": row.replacement,
        "reason": row.reason,
        "removed_in": row.removed_in,
    }


def _removed_section() -> list[str]:
    lines = [
        "## Removed and renamed keys\n",
        "A key listed here fails the load with `C111` (removed) or `C112` (renamed),",
        "naming what to write instead. `*` matches any one key or list item.\n",
    ]
    if not REMOVED_KEYS:
        lines.append("No keys have been removed or renamed yet.\n")
        return lines
    lines.append("| Kind | Key | Replacement or reason | Removed in |")
    lines.append("|------|-----|-----------------------|------------|")
    for row in REMOVED_KEYS:
        change = f"`{row.replacement}`" if row.replacement else row.reason
        lines.append(f"| {row.kind} | `{row.path}` | {change} | {row.removed_in} |")
    lines.append("")
    return lines


def _sort_key(entry: RegistryEntry) -> tuple[str, str]:
    return (entry.code[0], entry.code[1:])


def _stability(entry: RegistryEntry) -> str:
    if entry.stability is Stability.DEPRECATED:
        return f"deprecated (removed in {entry.removed_in})"
    return entry.stability.value


def render_page() -> str:
    """Render the reference page."""
    lines = [INTRO]
    by_block: dict[str, list[RegistryEntry]] = {}
    for entry in sorted(REGISTRY.values(), key=_sort_key):
        by_block.setdefault(entry.code[:2], []).append(entry)
    for block, entries in by_block.items():
        title = BLOCKS.get(block, block)
        lines.append(f"## {block}xx — {title}\n")
        for entry in entries:
            lines.append(f"### {entry.code}\n")
            lines.append(f"`{entry.name}` · {entry.severity.value} · {_stability(entry)}\n")
            lines.append(textwrap.fill(entry.summary, width=100) + "\n")
    lines.extend(_removed_section())
    if RETIRED_CODES:
        lines.append("## Retired codes\n")
        lines.append("These codes are no longer used and will not be reused.\n")
        lines.append(", ".join(f"`{code}`" for code in sorted(RETIRED_CODES)) + "\n")
    return "\n".join(lines)


def main() -> None:
    """Generate or check the code list."""
    outputs = {PAGE: render_page(), JSON_LIST: render_json()}
    if "--check" in sys.argv:
        stale = [path for path, text in outputs.items() if _read(path) != text]
        if not stale:
            print("Diagnostic code list is up-to-date.")
            return
        for path in stale:
            print(f"STALE:   {path.relative_to(ROOT)}")
        print(
            "\nThe diagnostic code list is out of date. The code registry in "
            "packages/weevr-core/src/weevr/diagnostics/registry.py changed without "
            "regenerating it.\n\n"
            "To fix, run locally:\n\n"
            "    uv run python scripts/generate_diagnostics.py\n\n"
            "Then commit docs/reference/diagnostics.md and docs/schema/diagnostics.json.",
            file=sys.stderr,
        )
        sys.exit(1)
    for path, text in outputs.items():
        path.write_text(text, encoding="utf-8")
    print(f"Wrote {PAGE.relative_to(ROOT)} and {JSON_LIST.relative_to(ROOT)}")


def _read(path: Path) -> str | None:
    return path.read_text(encoding="utf-8") if path.exists() else None


if __name__ == "__main__":
    main()
