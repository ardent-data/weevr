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

KIND_BY_SUFFIX: dict[str, str] = {
    ".thread": "thread",
    ".weave": "weave",
    ".loom": "loom",
    ".warp": "warp",
}

_ROOT_KIND = re.compile(r"^kind\s*:", re.M)


def with_kind(body: str | Mapping[str, Any], kind: str) -> str:
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
    resolved = kind or KIND_BY_SUFFIX.get(path.suffix.lower())
    if resolved is None:
        raise ValueError(f"write_document needs kind= for {path.name}")
    path.write_text(with_kind(body, resolved))
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


def write_project(root: Path, name: str = "test", project_id: str = TEST_PROJECT_ID) -> Path:
    """Make ``root`` a project directory: create it and write its ``weevr.yaml``.

    Args:
        root: The project directory; created with its parents when missing.
        name: The project name.
        project_id: The project id.

    Returns:
        ``root``.
    """
    root.mkdir(parents=True, exist_ok=True)
    (root / "weevr.yaml").write_text(project_yaml(name, project_id))
    return root
