"""Document discovery: list a project's documents with their kinds.

Discovery is a scan of the folder tree; nothing is indexed or cached, so
adding a file changes only that file's entry.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from weevr.config.locations import LocalConfigLocation
from weevr.config.parser import TYPED_EXTENSIONS, declared_kind
from weevr.config.root import project_root_not_found

_YAML_SUFFIXES = frozenset({".yaml", ".yml"})
_IGNORED_FILES = frozenset({".platform"})


@dataclass(frozen=True, order=True)
class DiscoveredDocument:
    """A document found under a project root.

    Attributes:
        path: The file's path relative to the root, with forward slashes.
        kind: The ``kind`` the document's envelope declares, or ``None`` for a
            typed-extension file that declares no known kind.
    """

    path: str
    kind: str | None


def discover_documents(root: str | Path | LocalConfigLocation) -> tuple[DiscoveredDocument, ...]:
    """List the documents under ``root``, sorted by path.

    A document is a ``.thread``, ``.weave``, ``.loom`` or ``.warp`` file, or a
    ``.yaml``/``.yml`` file that declares a known ``kind`` (``weevr.yaml``
    itself is listed as kind ``project``). A typed-extension file without a
    known kind is still listed, with kind ``None``, so tooling can report it.
    A ``.yaml``/``.yml`` file without a known kind is inert definition content
    and is not returned. ``.weevr/``, every dot-directory, and ``.platform``
    files are skipped. Only local roots are supported.

    Each candidate file is parsed to read its ``kind``, so the cost grows with
    the size of the YAML in the tree, not only the number of files.

    Args:
        root: The project directory.

    Returns:
        The documents, sorted by relative path.

    Raises:
        ConfigError: ``root`` is not an existing local directory (C209).
    """
    base = Path(os.fspath(root))
    if not base.is_dir():
        raise project_root_not_found(
            [LocalConfigLocation(base)],
            hint="Pass an existing local project directory.",
        )
    found: list[DiscoveredDocument] = []
    for dirpath, dirnames, filenames in os.walk(base):
        dirnames[:] = [d for d in dirnames if not d.startswith(".")]
        for filename in filenames:
            if filename in _IGNORED_FILES:
                continue
            suffix = Path(filename).suffix.lower()
            typed = suffix in TYPED_EXTENSIONS
            if not typed and suffix not in _YAML_SUFFIXES:
                continue
            file_path = Path(dirpath) / filename
            kind = declared_kind(LocalConfigLocation(file_path))
            if kind is None and not typed:
                continue
            found.append(DiscoveredDocument(file_path.relative_to(base).as_posix(), kind))
    return tuple(sorted(found))
