"""The committed diagnostic code list matches the code registry."""

from __future__ import annotations

import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
_spec = importlib.util.spec_from_file_location(
    "generate_diagnostics", ROOT / "scripts" / "generate_diagnostics.py"
)
assert _spec is not None and _spec.loader is not None
generator = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(generator)


def test_reference_page_is_current() -> None:
    assert generator.PAGE.read_text(encoding="utf-8") == generator.render_page(), (
        "Run `uv run python scripts/generate_diagnostics.py`"
    )


def test_json_list_is_current() -> None:
    assert generator.JSON_LIST.read_text(encoding="utf-8") == generator.render_json(), (
        "Run `uv run python scripts/generate_diagnostics.py`"
    )
