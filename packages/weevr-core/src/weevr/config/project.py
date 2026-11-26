"""Load-time checks of the project document beyond its schema.

The project-scope blocks a later release will define are reserved: found in a
``weevr.yaml`` they fail with their own code rather than as unknown keys. The
engine floor (``requires_weevr``) is checked against the installed weevr when
the document loads.
"""

from __future__ import annotations

from collections.abc import Mapping
from importlib.metadata import PackageNotFoundError, version
from typing import Any

from packaging.specifiers import SpecifierSet
from packaging.version import InvalidVersion, Version

from weevr.diagnostics import Diagnostic, diagnostic
from weevr.diagnostics.positions import json_pointer
from weevr.errors import ConfigError
from weevr.model.project import Project

RESERVED_PROJECT_BLOCKS: tuple[str, ...] = ("connections", "defaults", "audit_templates")
"""Root keys of ``weevr.yaml`` reserved for project scope."""


def split_reserved_blocks(raw: Mapping[str, Any]) -> tuple[dict[str, Any], list[Diagnostic]]:
    """Separate the reserved project-scope blocks from a project document.

    Args:
        raw: The project document as written.

    Returns:
        The document without its reserved blocks, and one ``C210`` per block found.
    """
    found = [key for key in RESERVED_PROJECT_BLOCKS if key in raw]
    rest = {key: value for key, value in raw.items() if key not in found}
    return rest, [
        diagnostic(
            "C210",
            f"'{key}' is reserved for the project scope and not yet supported in weevr.yaml",
            pointer=json_pointer((key,)),
            hint="Declare it in the documents that use it until project scope is supported.",
        )
        for key in found
    ]


def installed_weevr_version() -> str:
    """The installed weevr version, from the ``weevr-core`` distribution every install has."""
    return version("weevr-core")


def check_engine_floor(project: Project) -> None:
    """Fail when the installed weevr does not satisfy ``requires_weevr``.

    A pre-release install is compared by version order, as pip checks an
    installed version.

    Args:
        project: The validated project document.

    Raises:
        ConfigError: The installed version is outside the specifier, or
            cannot be determined (C208).
    """
    spec = project.requires_weevr
    if spec is None:
        return
    try:
        installed = installed_weevr_version()
        parsed = Version(installed)
    except (PackageNotFoundError, InvalidVersion) as exc:
        raise ConfigError.from_diagnostic(
            diagnostic(
                "C208",
                f"Cannot check requires_weevr '{spec}': the installed weevr version is "
                f"unknown ({exc})",
                pointer="/requires_weevr",
                expected=[spec],
                hint="Install weevr as a package (with its metadata), or remove requires_weevr.",
            ),
            cause=exc,
        ) from exc
    if SpecifierSet(spec).contains(parsed, prereleases=True):
        return
    raise ConfigError.from_diagnostic(
        diagnostic(
            "C208",
            f"Installed weevr {installed} does not satisfy requires_weevr '{spec}'",
            pointer="/requires_weevr",
            expected=[spec],
            hint="Install a weevr version the specifier allows, or relax requires_weevr.",
        )
    )
