"""Hydrate resolved config dicts into typed models, including referenced children."""

from collections.abc import Callable
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from weevr.config.attribution import attribute_errors
from weevr.config.locations import ConfigLocation
from weevr.config.macros import expand_foreach
from weevr.errors import ModelValidationError
from weevr.model import Loom, Thread, Weave

ThreadPreparer = Callable[[dict[str, Any]], None]
"""Hook applied to each child thread dict just before validation."""

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
        raise ModelValidationError(
            f"Model hydration failed for {config_type}: {exc}",
            cause=exc,
            file_path=str(source),
        ) from exc


def hydrate_threads(
    entries: list[Any],
    resolved: list[dict[str, Any]],
    source: ConfigLocation | str,
    prepare: ThreadPreparer | None = None,
) -> dict[str, Thread]:
    """Hydrate a weave's resolved threads into a name-to-Thread mapping.

    Threads loaded from their own file are attributed to that file; inline
    threads are attributed to ``source``, the enclosing file. ``foreach``
    blocks in each thread's steps are expanded before validation.

    Args:
        entries: The weave's ``threads`` entries, aligned with ``resolved``.
        resolved: Resolved thread dicts from reference resolution.
        source: The weave's file.
        prepare: Optional hook applied to each thread dict before validation.

    Returns:
        Hydrated threads keyed by effective name (``as`` > name > stem).

    Raises:
        ModelValidationError: If a thread fails model validation.
    """
    result: dict[str, Thread] = {}
    for entry, thread_dict in zip(entries, resolved, strict=True):
        # Pop stashed entry-level fields before model validation.
        # _entry_params was already consumed by the resolver during
        # two-phase param resolution; popped here for model cleanliness.
        entry_as = thread_dict.pop("_entry_as", None)
        thread_dict.pop("_entry_params", None)
        thread_source = thread_dict.pop("_source_path", None) or str(source)

        # Effective name: as > name > stem
        effective_name = entry_as or _entry_name(entry)

        if not thread_dict.get("name") or entry_as:
            thread_dict["name"] = effective_name

        # Set template_ref for telemetry provenance
        ref = entry.get("ref") if isinstance(entry, dict) else None
        if ref:
            thread_dict["template_ref"] = ref

        # Expand foreach macros, as a directly loaded thread does at step 8b.
        steps = thread_dict.get("steps")
        if isinstance(steps, list):
            with attribute_errors(thread_source):
                thread_dict["steps"] = expand_foreach(steps)

        if prepare is not None:
            prepare(thread_dict)

        try:
            result[effective_name] = Thread.model_validate(thread_dict)
        except ValidationError as exc:
            raise ModelValidationError(
                f"Thread hydration failed for '{effective_name}': {exc}",
                cause=exc,
                file_path=thread_source,
            ) from exc
    return result


def hydrate_children(
    config_type: str,
    resolved: dict[str, Any],
    source: ConfigLocation | str,
    name: str,
    prepare: ThreadPreparer | None = None,
) -> tuple[dict[str, Weave], dict[str, dict[str, Thread]]]:
    """Hydrate the weaves and threads a weave or loom pulls in.

    Consumes the private keys reference resolution stashed on the child
    dicts, so call it after hydrating the top-level model.

    Args:
        config_type: Type of the top-level config.
        resolved: The top-level config after reference resolution and the
            inheritance cascade.
        source: The top-level config's file.
        name: The top-level config's name, used as the weave key for a weave.
        prepare: Optional hook applied to each thread dict before validation.

    Returns:
        ``(weaves, threads)``: hydrated weaves by name (looms only), and each
        weave's hydrated threads by weave name. Both are empty for a thread.

    Raises:
        ModelValidationError: If any child fails model validation. The error
            names the child's own file, or the enclosing file for inline
            definitions.
    """
    weaves: dict[str, Weave] = {}
    threads: dict[str, dict[str, Thread]] = {}

    if config_type == "weave":
        threads[name] = hydrate_threads(
            resolved.get("threads", []),
            resolved.get("_resolved_threads", []),
            source,
            prepare,
        )

    elif config_type == "loom":
        for weave_entry, weave_dict in zip(
            resolved.get("weaves", []),
            resolved.get("_resolved_weaves", []),
            strict=True,
        ):
            weave_name = _entry_name(weave_entry)

            if not weave_dict.get("name"):
                weave_dict["name"] = weave_name
            # Inline weaves have no file of their own; they live in the loom.
            weave_source = weave_dict.pop("_source_path", None) or str(source)
            weave_model = hydrate_model(weave_dict, "weave", weave_source)
            if not isinstance(weave_model, Weave):  # pragma: no cover
                continue
            weaves[weave_name] = weave_model

            threads[weave_name] = hydrate_threads(
                weave_dict.get("threads", []),
                weave_dict.get("_resolved_threads", []),
                weave_source,
                prepare,
            )

    return weaves, threads
