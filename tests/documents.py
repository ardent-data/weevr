"""Write config documents for tests, with the ``kind`` of their envelope filled in.

Test bodies carry ``config_version`` themselves; :func:`write_document` adds the
``kind`` line, taken from the file's typed extension (or given for a
``.yaml``/``.yml`` document). A body that already declares a root ``kind`` is
written unchanged, so tests of a wrong or missing kind write their own.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import yaml

from weevr.config.parser import TYPED_EXTENSIONS

_ROOT_KIND = re.compile(r"^kind\s*:", re.M)


def _with_kind(body: str | Mapping[str, Any], kind: str) -> str:
    """Return ``body`` as YAML text that declares ``kind`` first, unless it declares one.

    Args:
        body: YAML text (JSON text stays JSON), or a mapping dumped as YAML.
        kind: The document kind.

    Returns:
        The document text.
    """
    if isinstance(body, Mapping):
        if "kind" in body:
            return yaml.safe_dump(dict(body), sort_keys=False)
        return yaml.safe_dump({"kind": kind, **body}, sort_keys=False)
    if body.lstrip().startswith("{"):
        data = json.loads(body)
        return body if "kind" in data else json.dumps({"kind": kind, **data})
    if _ROOT_KIND.search(body):
        return body
    return f"kind: {kind}\n{body}"


def write_document(path: Path, body: str | Mapping[str, Any], *, kind: str | None = None) -> Path:
    """Write a config document to ``path``, declaring its kind.

    Args:
        path: Where to write; a typed extension gives the kind.
        body: YAML text, or a mapping dumped as YAML.
        kind: The kind, required for a ``.yaml``/``.yml`` document.

    Returns:
        ``path``.

    Raises:
        ValueError: ``path`` has no typed extension and no ``kind`` was given.
    """
    resolved = kind or TYPED_EXTENSIONS.get(path.suffix.lower())
    if resolved is None:
        raise ValueError(f"write_document needs kind= for {path.name}")
    path.write_text(_with_kind(body, resolved))
    return path


TEST_PROJECT_ID = "0b5f3c1e-2d4a-4b6c-8e9f-1a2b3c4d5e6f"
"""The ``id`` of the projects :func:`write_project` writes."""


def project_yaml(name: str = "test", project_id: str = TEST_PROJECT_ID) -> str:
    """Return the text of a minimal ``weevr.yaml``.

    Args:
        name: The project name.
        project_id: The project id.

    Returns:
        The document text.
    """
    return f'kind: project\nconfig_version: "2.0"\nid: {project_id}\nname: {name}\n'


def write_project(
    root: Path, name: str = "test", project_id: str = TEST_PROJECT_ID, **extra: Any
) -> Path:
    """Make ``root`` a project directory: create it and write its ``weevr.yaml``.

    Args:
        root: The project directory; created with its parents when missing.
        name: The project name.
        project_id: The project id.
        **extra: Further top-level blocks (``connections``, ``lookups``, ...), dumped
            as YAML after the identity keys in the order given.

    Returns:
        ``root``; the file is ``root / "weevr.yaml"``.
    """
    root.mkdir(parents=True, exist_ok=True)
    text = project_yaml(name, project_id)
    if extra:
        text += yaml.safe_dump(extra, sort_keys=False)
    (root / "weevr.yaml").write_text(text)
    return root


def lake_connection(workspace: str, lakehouse: str = "lh") -> dict[str, Any]:
    """A OneLake connection body.

    Args:
        workspace: The workspace.
        lakehouse: The lakehouse.

    Returns:
        The connection mapping.
    """
    return {"type": "onelake", "workspace": workspace, "lakehouse": lakehouse}


def delta_lookup(alias: str, **extra: Any) -> dict[str, Any]:
    """A lookup reading the delta table ``alias``.

    Args:
        alias: The source table alias.
        **extra: Further lookup keys.

    Returns:
        The lookup mapping.
    """
    return {"source": {"type": "delta", "alias": alias}, **extra}


def delta_column_set(alias: str) -> dict[str, Any]:
    """A column set read from the delta table ``alias``.

    Args:
        alias: The source table alias.

    Returns:
        The column-set mapping.
    """
    return {"source": {"type": "delta", "alias": alias}}


def write_named(root: Path, name: str, body: str | Mapping[str, Any]) -> Path:
    """Write the document ``name`` under ``root`` (:func:`write_document`).

    Args:
        root: The directory to write in.
        name: The file name; its typed extension gives the kind.
        body: YAML text, or a mapping dumped as YAML.

    Returns:
        The written path.
    """
    return write_document(root / name, body)


def thread_body(alias: str = "out.t", **extra: Any) -> dict[str, Any]:
    """A minimal thread: one delta source (``raw.t``) and a target at ``alias``.

    Args:
        alias: The target alias.
        **extra: Further top-level keys; a ``target`` replaces the default one.

    Returns:
        The thread body.
    """
    return {
        "config_version": "2.0",
        "sources": {"s": {"type": "delta", "alias": "raw.t"}},
        "target": {"alias": alias},
        **extra,
    }


def weave_body(threads: list[Any], **extra: Any) -> dict[str, Any]:
    """A weave over ``threads``, with ``extra`` top-level keys."""
    return {"config_version": "2.0", "threads": threads, **extra}


def loom_body(weaves: list[Any], **extra: Any) -> dict[str, Any]:
    """A loom over ``weaves``, with ``extra`` top-level keys."""
    return {"config_version": "2.0", "weaves": weaves, **extra}


def string_spec(name: str, **fields: Any) -> dict[str, Any]:
    """One ``string`` param spec named ``name``, with ``fields`` over it."""
    return {"name": name, "type": "string", **fields}


def string_specs(**specs: dict[str, Any]) -> dict[str, Any]:
    """A ``params:`` block of ``string`` specs, each name's fields over its spec."""
    return {name: string_spec(name, **fields) for name, fields in specs.items()}
