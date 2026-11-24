"""Project roots: the folder holding ``weevr.yaml``.

A project is rooted by its ``weevr.yaml`` (``kind: project``). ``Context``
requires one at the project directory it is given; a core-API load of a bare
file walks up from the file to the nearest folder holding one, and with none
roots the load at the file's folder with no project.
"""

from __future__ import annotations

import os
from pathlib import Path

from weevr.config.document_load import load_document
from weevr.config.locations import ConfigLocation, LocalConfigLocation, RemoteConfigLocation
from weevr.diagnostics import diagnostic
from weevr.errors import ConfigError
from weevr.model.project import Project

PROJECT_FILE = "weevr.yaml"
"""The file that marks a project root."""

_REMOTE_WALK_BOUNDARY = "Files"
"""The lakehouse or item folder a remote walk never climbs above."""


def find_project_root(location: ConfigLocation) -> ConfigLocation | None:
    """Return the nearest folder at or above ``location`` that holds ``weevr.yaml``.

    The walk starts at ``location`` itself (so a project directory finds
    itself, and a file starts one probe early) and climbs parent by parent.
    A local walk uses the absolute path, so a relative location sees the
    real ancestors of the working directory, and ends at the filesystem
    root. A remote walk ends at the URI's authority root, and never climbs
    above a ``Files`` folder: a project in a lakehouse or item lives under
    ``Files/``, so the folders above it are never probed.

    Args:
        location: A config file or directory.

    Returns:
        The root folder (absolute, for a local location), or ``None`` when no
        folder on the way up holds ``weevr.yaml``.

    Raises:
        OSError: A remote existence probe failed.
    """
    current = (
        LocalConfigLocation(Path(os.path.abspath(location.path)))
        if isinstance(location, LocalConfigLocation)
        else location
    )
    while True:
        if current.join(PROJECT_FILE).exists():
            return current
        if isinstance(current, RemoteConfigLocation) and current.name == _REMOTE_WALK_BOUNDARY:
            return None
        parent = current.parent
        if parent == current:
            return None
        current = parent


def read_project(root: ConfigLocation) -> Project | None:
    """Load the ``weevr.yaml`` at ``root``, or return ``None`` when it has none.

    Args:
        root: A project directory.

    Returns:
        The validated project document, or ``None``.

    Raises:
        ConfigError: The file fails to load, or declares a kind other than
            ``project`` (C207).
    """
    location = root.join(PROJECT_FILE)
    if not location.exists():
        return None
    kind, model = load_document(location)
    if not isinstance(model, Project):
        raise ConfigError.from_diagnostic(
            diagnostic(
                "C207",
                f"{PROJECT_FILE} declares kind '{kind}'; a project root's {PROJECT_FILE} "
                "must declare kind 'project'",
                file=str(location),
                pointer="/kind",
                expected=["project"],
                hint=f"Set 'kind: project' in {PROJECT_FILE}, or rename the file.",
            )
        )
    return model


def project_root_not_found(probed: list[ConfigLocation], hint: str | None = None) -> ConfigError:
    """The C209 error for a project directory without ``weevr.yaml``.

    Args:
        probed: Every ``weevr.yaml`` location that was tried.
        hint: What to change, replacing the default advice.

    Returns:
        The error, listing the probed locations.
    """
    listed = ", ".join(str(p) for p in probed)
    return ConfigError.from_diagnostic(
        diagnostic(
            "C209",
            f"No {PROJECT_FILE} found for the project; probed: {listed}",
            expected=[str(p) for p in probed],
            hint=hint
            or (
                f"Add a {PROJECT_FILE} (kind: project) at the project root, or point the "
                "Context at the folder that holds it."
            ),
        )
    )
