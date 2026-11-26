"""Load-time checks of the project document beyond its schema.

The project-scope ``variables`` block is reserved: found in a ``weevr.yaml`` it
fails with its own code rather than as an unknown key. The project's
connections resolve on their own, outside any load, for the run store. The
engine floor (``requires_weevr``) is checked against the installed weevr when
the document loads.
"""

from __future__ import annotations

from collections.abc import Collection, Mapping
from importlib.metadata import PackageNotFoundError, version
from typing import Any

from packaging.specifiers import SpecifierSet
from packaging.version import InvalidVersion, Version

from weevr.config.interpolation import check_fabric_context
from weevr.config.params import ParamScope
from weevr.config.scoping import WalkState
from weevr.diagnostics import Diagnostic, diagnostic
from weevr.diagnostics.positions import json_pointer
from weevr.errors import ConfigError
from weevr.model.connection import OneLakeConnection
from weevr.model.project import Project

RESERVED_PROJECT_BLOCKS: tuple[str, ...] = ("variables",)
"""Root keys of ``weevr.yaml`` reserved for project scope."""


def split_reserved_blocks(raw: Mapping[str, Any]) -> tuple[dict[str, Any], list[Diagnostic]]:
    """Separate the reserved project-scope blocks (``variables``) from a project document.

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
            hint="Declare it in the looms, weaves or threads that use it.",
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


def resolve_project_connections(
    project: Project,
    project_file: str | None,
    *,
    runtime_params: Mapping[str, Any] | None = None,
    fabric_context: Mapping[str, Any] | None = None,
    strict_params: bool | None = None,
    names: Collection[str] | None = None,
) -> dict[str, OneLakeConnection] | None:
    """The project's ``connections``, resolved in the project's own scope.

    Resolves as a load resolves them (:meth:`WalkState.project_tier`): a
    ``${param.x}`` reads the runtime value over the project's default, and a
    ``${fabric.x}`` the Fabric context. Only the connections are resolved,
    and with ``names`` only those named, with the params they read; the
    project's other blocks are left for a load. Load warnings are not
    reported here.

    Args:
        project: The project document.
        project_file: The project's ``weevr.yaml``, named by errors.
        runtime_params: The invoker's runtime params.
        fabric_context: The Fabric values for ``${fabric.x}``; ``None`` leaves
            each such reference in place.
        strict_params: Overrides the project's ``strict_params``.
        names: The connections to resolve; ``None`` resolves every one.

    Returns:
        The connections by name, or ``None`` when the project declares none
        (of ``names``, when given).

    Raises:
        ConfigError: A reference can't resolve (``P110``), or a project param
            the connections read has no value (``P101``) or a value its type
            rejects.
    """
    check_fabric_context(fabric_context)
    if names is not None:
        wanted = {n: c for n, c in (project.connections or {}).items() if n in names}
        project = project.model_copy(update={"connections": wanted})
    walk = WalkState(
        scope=ParamScope.root(),
        file=project_file,
        strict_override=strict_params,
        runtime_params=runtime_params,
        fabric_context=fabric_context,
    )
    connections = walk.project_tier(project, project_file, {"connections"}).get("connections")
    if not connections:
        return None
    return {name: OneLakeConnection.model_validate(body) for name, body in connections.items()}
