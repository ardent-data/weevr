#!/usr/bin/env python3
"""Generate JSON Schema files from weevr Pydantic models.

Exports Thread, Weave, and Loom schemas to docs/schema/ as individual
JSON files plus a combined weevr.json bundle.

Usage:
    python scripts/generate_schema.py          # Generate schema files
    python scripts/generate_schema.py --check  # Verify schemas are up-to-date
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

SCHEMA_DIR = Path(__file__).resolve().parent.parent / "docs" / "schema"

MODELS = {
    "thread": "weevr.model.thread:Thread",
    "weave": "weevr.model.weave:Weave",
    "loom": "weevr.model.loom:Loom",
    "warp": "weevr.model.warp:WarpConfig",
}


def _import_model(dotted: str) -> type:
    """Import a model class from a dotted module:class path."""
    module_path, class_name = dotted.rsplit(":", 1)
    module = __import__(module_path, fromlist=[class_name])
    return getattr(module, class_name)


def generate() -> dict[str, dict]:
    """Generate JSON Schema dicts for all registered models."""
    schemas: dict[str, dict] = {}
    for name, dotted in MODELS.items():
        try:
            model_cls = _import_model(dotted)
        except (ImportError, AttributeError) as exc:
            print(f"ERROR: failed to import {dotted}: {exc}", file=sys.stderr)
            sys.exit(1)
        schemas[name] = model_cls.model_json_schema()
    return schemas


def write_schemas(schemas: dict[str, dict]) -> None:
    """Write individual and combined schema files to docs/schema/."""
    SCHEMA_DIR.mkdir(parents=True, exist_ok=True)

    for name, schema in schemas.items():
        path = SCHEMA_DIR / f"{name}.json"
        path.write_text(
            json.dumps(schema, indent=2, sort_keys=False) + "\n",
            encoding="utf-8",
        )

    combined = SCHEMA_DIR / "weevr.json"
    combined.write_text(
        json.dumps(schemas, indent=2, sort_keys=False) + "\n",
        encoding="utf-8",
    )


def check_schemas(schemas: dict[str, dict]) -> bool:
    """Compare generated schemas against committed files. Returns True if up-to-date."""
    all_ok = True

    for name, schema in schemas.items():
        path = SCHEMA_DIR / f"{name}.json"
        expected = json.dumps(schema, indent=2, sort_keys=False) + "\n"
        if not path.exists():
            print(f"MISSING: {path}")
            all_ok = False
        elif path.read_text(encoding="utf-8") != expected:
            print(f"STALE:   {path}")
            all_ok = False

    combined = SCHEMA_DIR / "weevr.json"
    expected_combined = json.dumps(schemas, indent=2, sort_keys=False) + "\n"
    if not combined.exists():
        print(f"MISSING: {combined}")
        all_ok = False
    elif combined.read_text(encoding="utf-8") != expected_combined:
        print(f"STALE:   {combined}")
        all_ok = False

    return all_ok


def main() -> None:
    """Generate or check JSON Schema files."""
    check_mode = "--check" in sys.argv
    schemas = generate()

    if check_mode:
        if check_schemas(schemas):
            print("JSON schemas are up-to-date.")
        else:
            print(
                "\nJSON schemas are out of date. This usually means a model "
                "file under packages/weevr/src/weevr/model/ was changed without regenerating "
                "the schemas.\n\n"
                "To fix, run locally:\n\n"
                "    uv run python scripts/generate_schema.py\n\n"
                "Then commit the updated files in docs/schema/.",
                file=sys.stderr,
            )
            sys.exit(1)
    else:
        write_schemas(schemas)
        print(f"Generated {len(schemas) + 1} schema files in {SCHEMA_DIR}/")


if __name__ == "__main__":
    main()
