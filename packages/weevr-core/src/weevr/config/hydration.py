"""Hydrate resolved config dicts into typed models, including referenced children."""

from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from weevr.config.attribution import attribute_errors
from weevr.config.inheritance import cascade_registry
from weevr.config.locations import ConfigLocation
from weevr.config.macros import expand_foreach, reject_foreach_in_with
from weevr.config.resolver import ResolvedChild
from weevr.config.validation import (
    split_diagnostics,
    validate_rename_column_sets,
    validate_resolve_lookups,
    validate_source_lookups,
)
from weevr.diagnostics.pydantic import from_validation_error
from weevr.errors import ConfigError, ModelValidationError
from weevr.model import Loom, Thread, Weave

ChildPreparer = Callable[[dict[str, Any]], None]
"""Hook applied to each child thread dict, and each loom child weave dict, before validation."""

ThreadBinder = Callable[[Thread, str], None]
"""Hook called with each hydrated thread and the file it came from (path or URI)."""

_MODELS: dict[str, type[Thread | Weave | Loom]] = {
    "thread": Thread,
    "weave": Weave,
    "loom": Loom,
}


def _entry_name(entry: Any) -> str:
    """Name a thread or weave entry: its explicit ``name``, else its ``ref``'s stem."""
    if isinstance(entry, dict):
        if entry.get("ref") and not entry.get("name"):
            return Path(entry["ref"]).stem
        return entry.get("name", "") or entry.get("ref", "")
    return str(entry)


def _weave_origin(entry: Any) -> str:
    """Describe where a loom's weave entry comes from, for error messages."""
    if isinstance(entry, dict) and entry.get("ref"):
        return f"'{entry['ref']}'"
    if isinstance(entry, dict) and entry.get("threads") is not None:
        return "an inline weave"
    return f"'{_entry_name(entry)}.weave'"


def _effective_names(outer: Mapping[str, Any] | None, own: Any, resource_type: str) -> set[str]:
    """Names a thread sees: its own registry cascaded under its enclosing scope's."""
    own_registry = own if isinstance(own, Mapping) else None
    merged = cascade_registry(
        outer, own_registry, resource_type, "weave", "thread", log_shadowing=False
    )
    return set(merged or {})


def check_resolve_lookups(
    thread_dict: dict[str, Any],
    outer: Mapping[str, Any] | None,
    source: ConfigLocation | str,
    name: str,
    warnings: list[str] | None,
    *,
    standalone: bool = False,
) -> None:
    """Check a thread's lookup references against the lookups it can see at run time.

    ``outer`` is the enclosing scope's effective lookups (the weave's merged
    over the loom's); the thread's own are cascaded under it with the same
    rule the tree uses, and resolve steps also see its lookup-bound source
    aliases. ``standalone`` marks a thread loaded from its own
    file: its source lookups may come from a weave or loom it can't see, so
    those warn. See :func:`~weevr.config.validation.validate_source_lookups`.

    Raises:
        ConfigError: For a lookup no scope defines (single mode with
            ``on_failure: warn`` adds a warning to ``warnings`` instead).
    """
    sources = thread_dict.get("sources") or {}
    defined = _effective_names(outer, thread_dict.get("lookups"), "lookup")
    available = defined | {
        alias for alias, src in sources.items() if isinstance(src, dict) and src.get("lookup")
    }
    errors, advisories = split_diagnostics(
        validate_source_lookups(thread_dict, available_lookups=defined, standalone=standalone)
        + validate_resolve_lookups(thread_dict, available_lookups=available)
    )
    if errors:
        raise ConfigError("; ".join(errors), file_path=str(source))
    if warnings is not None:
        warnings.extend(f"WARN: thread '{name}': {message}" for message in advisories)


def check_column_set_references(
    thread_dict: dict[str, Any],
    outer: Mapping[str, Any] | None,
    source: ConfigLocation | str,
    name: str,
    warnings: list[str] | None,
    *,
    standalone: bool = False,
) -> None:
    """Check a thread's rename steps against the column sets it can see at run time.

    ``outer`` is the enclosing scope's effective column sets; the thread's
    own are cascaded under it with the same rule the tree uses.
    ``standalone`` marks a thread loaded from its own file, where a set may
    come from a weave or loom it can't see, so a missing name warns. See
    :func:`~weevr.config.validation.validate_rename_column_sets`.

    Raises:
        ConfigError: For a column set no visible scope defines.
    """
    available = _effective_names(outer, thread_dict.get("column_sets"), "column_set")
    errors, advisories = split_diagnostics(
        validate_rename_column_sets(
            thread_dict, available_column_sets=available, standalone=standalone
        )
    )
    if errors:
        raise ConfigError(
            "; ".join(f"thread '{name}': {message}" for message in errors),
            file_path=str(source),
        )
    if warnings is not None:
        warnings.extend(f"WARN: thread '{name}': {message}" for message in advisories)


def hydrate_model(
    data: dict[str, Any], config_type: str, source: ConfigLocation | str
) -> Thread | Weave | Loom:
    """Validate a resolved config dict into its typed model.

    Args:
        data: Fully resolved config dict.
        config_type: ``'thread'``, ``'weave'``, or ``'loom'``.
        source: The file the config came from, named on failure.

    Returns:
        The hydrated model.

    Raises:
        ModelValidationError: If the dict fails model validation.
    """
    try:
        return _MODELS[config_type].model_validate(data)
    except ValidationError as exc:
        raise _hydration_error(
            f"Model hydration failed for {config_type}: {exc}", exc, data, str(source)
        ) from exc


def _hydration_error(
    message: str, exc: ValidationError, data: dict[str, Any], file: str
) -> ModelValidationError:
    """A ``ModelValidationError`` carrying one coded diagnostic per pydantic error."""
    return ModelValidationError(
        message,
        cause=exc,
        file_path=file,
        diagnostics=from_validation_error(exc, data, file=file),
    )


def hydrate_threads(
    children: list[ResolvedChild],
    source: ConfigLocation | str,
    prepare: ChildPreparer | None = None,
    outer_lookups: Mapping[str, Any] | None = None,
    warnings: list[str] | None = None,
    bind: ThreadBinder | None = None,
    outer_column_sets: Mapping[str, Any] | None = None,
) -> dict[str, Thread]:
    """Hydrate a weave's resolved threads into a name-to-Thread mapping.

    Threads loaded from their own file are attributed to that file; inline
    threads are attributed to ``source``, the enclosing file. ``foreach``
    blocks in each thread's steps are expanded before validation.

    Args:
        children: The weave's resolved threads.
        source: The weave's file.
        prepare: Optional hook applied to each thread dict before validation.
        outer_lookups: The enclosing scope's effective lookups; each
            thread's lookup references are checked against them and its own.
        warnings: Collects load warnings from those checks.
        bind: Optional hook called with each hydrated thread and its file.
        outer_column_sets: The enclosing scope's effective column sets; each
            thread's rename steps are checked against them and its own.

    Returns:
        Hydrated threads keyed by effective name (``as`` > name > stem).

    Raises:
        ConfigError: If a resolve step names a lookup, or a rename step a
            column set, that no scope defines.
        ModelValidationError: If a thread fails model validation.
    """
    result: dict[str, Thread] = {}
    for child in children:
        entry = child.entry
        thread_dict = child.data
        entry_as = entry.get("as")
        thread_source = child.source_path or str(source)

        # Effective name: as > name > stem
        effective_name = entry_as or _entry_name(entry)

        if not thread_dict.get("name") or entry_as:
            thread_dict["name"] = effective_name

        # Set template_ref for telemetry provenance
        ref = entry.get("ref")
        if ref:
            thread_dict["template_ref"] = ref

        # Expand foreach macros, as a directly loaded thread does at step 8b.
        steps = thread_dict.get("steps")
        with attribute_errors(thread_source):
            reject_foreach_in_with(thread_dict)
            if isinstance(steps, list):
                thread_dict["steps"] = expand_foreach(steps)

        check_resolve_lookups(thread_dict, outer_lookups, thread_source, effective_name, warnings)
        check_column_set_references(
            thread_dict, outer_column_sets, thread_source, effective_name, warnings
        )

        if prepare is not None:
            prepare(thread_dict)

        try:
            thread = Thread.model_validate(thread_dict)
        except ValidationError as exc:
            raise _hydration_error(
                f"Thread hydration failed for '{effective_name}': {exc}",
                exc,
                thread_dict,
                thread_source,
            ) from exc
        if bind is not None:
            bind(thread, thread_source)
        result[effective_name] = thread
    return result


def hydrate_children(
    config_type: str,
    resolved: dict[str, Any],
    children: list[ResolvedChild],
    source: ConfigLocation | str,
    name: str,
    prepare: ChildPreparer | None = None,
    warnings: list[str] | None = None,
    bind: ThreadBinder | None = None,
) -> tuple[dict[str, Weave], dict[str, dict[str, Thread]]]:
    """Hydrate the weaves and threads a weave or loom pulls in.

    ``prepare`` mutates the child dicts in place, so hydrate each resolved
    config once.

    Args:
        config_type: Type of the top-level config.
        resolved: The top-level config after reference resolution.
        children: Its resolved children, after the inheritance cascade.
        source: The top-level config's file.
        name: The top-level config's name, used as the weave key for a weave.
        prepare: Optional hook applied to each child thread dict, and to each
            child weave dict of a loom, before validation.
        warnings: Collects load warnings from the children's checks.
        bind: Optional hook called with each hydrated thread and its file.

    Returns:
        ``(weaves, threads)``: hydrated weaves by name (looms only), and each
        weave's hydrated threads by weave name. Both are empty for a thread.

    Raises:
        ConfigError: If two weave entries in a loom resolve to the same name,
            or a child thread's resolve step names a lookup, or its rename
            step a column set, that no scope defines.
        ModelValidationError: If any child fails model validation. The error
            names the child's own file, or the enclosing file for inline
            definitions.
    """
    weaves: dict[str, Weave] = {}
    threads: dict[str, dict[str, Thread]] = {}

    if config_type == "weave":
        threads[name] = hydrate_threads(
            children,
            source,
            prepare,
            outer_lookups=resolved.get("lookups"),
            warnings=warnings,
            bind=bind,
            outer_column_sets=resolved.get("column_sets"),
        )

    elif config_type == "loom":
        # Weaves are keyed by name from here on (planner, runner, results),
        # so a second entry with the same name would silently replace the first.
        defined_by: dict[str, str] = {}
        for weave in children:
            weave_entry = weave.entry
            weave_dict = weave.data
            weave_name = _entry_name(weave_entry)
            origin = _weave_origin(weave_entry)
            if weave_name in defined_by:
                raise ConfigError(
                    f"Duplicate weave name '{weave_name}' in loom: {defined_by[weave_name]} "
                    f"and {origin}. Give one entry a unique 'name:'."
                )
            defined_by[weave_name] = origin

            if not weave_dict.get("name"):
                weave_dict["name"] = weave_name
            # Inline weaves have no file of their own; they live in the loom.
            weave_source = weave.source_path or str(source)
            if prepare is not None:
                prepare(weave_dict)
            weave_model = hydrate_model(weave_dict, "weave", weave_source)
            if not isinstance(weave_model, Weave):  # pragma: no cover
                continue
            weaves[weave_name] = weave_model

            threads[weave_name] = hydrate_threads(
                weave.children,
                weave_source,
                prepare,
                outer_lookups=cascade_registry(
                    resolved.get("lookups"),
                    weave_dict.get("lookups"),
                    "lookup",
                    "loom",
                    "weave",
                    log_shadowing=False,
                ),
                warnings=warnings,
                bind=bind,
                outer_column_sets=cascade_registry(
                    resolved.get("column_sets"),
                    weave_dict.get("column_sets"),
                    "column_set",
                    "loom",
                    "weave",
                    log_shadowing=False,
                ),
            )

    return weaves, threads
