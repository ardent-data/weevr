"""Pre-resolution schema validation for the config loading pipeline.

These loose schemas validate config structure *before* variable substitution.
They intentionally use ``dict[str, Any]`` and ``extra="allow"`` so that
``${...}`` placeholder strings pass validation.  Strict domain-model
validation happens as the final step of ``load_config()`` via
``model_validate()``.

Post-resolution cross-cutting validators run after variable substitution on
resolved thread config dicts: ``validate_dimension_overrides`` and
``validate_resolve_lookups`` during the shared config load (the dimension rules on
each thread's own fields, before the inheritance cascade), and
``validate_incremental_config`` at executor entry.
"""

from __future__ import annotations

import json
import re
from collections.abc import Sequence
from dataclasses import replace
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, Field, ValidationError

from weevr.config.project import split_reserved_blocks
from weevr.diagnostics import Diagnostic, Severity, diagnostic
from weevr.diagnostics.positions import json_pointer, pointed
from weevr.diagnostics.pydantic import from_validation_error
from weevr.diagnostics.removed_keys import removed_key_diagnostics
from weevr.errors import ConfigSchemaError
from weevr.model.execution import resolve_effective_execution
from weevr.model.params import ParamsConfig
from weevr.model.project import Project
from weevr.model.warp import WarpConfig

if TYPE_CHECKING:
    from weevr.model.thread import Thread


class BaseConfig(BaseModel):
    """Base configuration model with shared fields."""

    model_config = {"extra": "allow"}

    config_version: str
    params: dict[str, Any] | None = None
    defaults: dict[str, Any] | None = None


class ThreadConfig(BaseConfig):
    """Thread configuration schema (pre-resolution, Phase 0 fields)."""

    sources: dict[str, Any]
    steps: list[dict[str, Any]] = Field(default_factory=list)
    target: dict[str, Any]
    write: dict[str, Any] | None = None
    keys: dict[str, Any] | None = None
    validations: list[dict[str, Any]] | None = None
    assertions: list[dict[str, Any]] | None = None
    load: dict[str, Any] | None = None
    tags: list[str] | None = None
    exports: list[dict[str, Any]] | None = None
    execution: dict[str, Any] | None = None
    connections: dict[str, Any] | None = None
    audit_templates: dict[str, Any] | None = None
    lookups: dict[str, Any] | None = None
    column_sets: dict[str, Any] | None = None
    variables: dict[str, Any] | None = None
    pre_steps: list[dict[str, Any]] | None = None
    post_steps: list[dict[str, Any]] | None = None
    with_: dict[str, Any] | None = Field(
        default=None,
        alias="with",
        description="Named sub-pipelines (CTEs) resolved before the main steps pipeline.",
    )


class WeaveConfig(BaseConfig):
    """Weave configuration schema (pre-resolution)."""

    threads: list[str | dict[str, Any]]
    defaults: dict[str, Any] | None = None
    execution: dict[str, Any] | None = None
    lookups: dict[str, Any] | None = None
    column_sets: dict[str, Any] | None = None
    variables: dict[str, Any] | None = None
    connections: dict[str, Any] | None = None
    pre_steps: list[dict[str, Any]] | None = None
    post_steps: list[dict[str, Any]] | None = None
    audit_templates: dict[str, Any] | None = None


class LoomConfig(BaseConfig):
    """Loom configuration schema (pre-resolution)."""

    weaves: list[str | dict[str, Any]]
    defaults: dict[str, Any] | None = None
    execution: dict[str, Any] | None = None
    column_sets: dict[str, Any] | None = None
    lookups: dict[str, Any] | None = None
    variables: dict[str, Any] | None = None
    connections: dict[str, Any] | None = None
    pre_steps: list[dict[str, Any]] | None = None
    post_steps: list[dict[str, Any]] | None = None
    audit_templates: dict[str, Any] | None = None


def _dumped_to_authored() -> dict[str, str]:
    """Field names the pre-resolution schemas dump under a name other than the authored key."""
    names: dict[str, str] = {}
    for model in (ThreadConfig, WeaveConfig, LoomConfig, ParamsConfig, WarpConfig):
        for name, info in model.model_fields.items():
            if info.alias and info.alias != name:
                names[name] = info.alias
    return names


def authored_path(segments: Sequence[str | int]) -> tuple[str | int, ...]:
    """A path into a schema-dumped config, its field keys as the author wrote them.

    Only a config's own fields are renamed (``with_`` → ``with``): the first
    segment, or the one right after ``threads/<n>`` for an inline thread. A key
    the author named ``with_`` inside a free-form mapping (columns, params) is
    left as it is.

    Args:
        segments: Keys and list indexes into the dict ``validate_schema(...).model_dump()`` returns.

    Returns:
        The same path in the authored document.
    """
    path = list(segments)
    for index, segment in enumerate(path):
        is_field = index == 0 or (
            index >= 2 and path[index - 2] == "threads" and isinstance(path[index - 1], int)
        )
        if is_field and isinstance(segment, str):
            path[index] = _DUMPED_TO_AUTHORED.get(segment, segment)
    return tuple(path)


_DUMPED_TO_AUTHORED: dict[str, str] = _dumped_to_authored()


def validate_schema(
    raw: dict[str, Any], config_type: str, *, at: tuple[str | int, ...] = ()
) -> BaseModel:
    """Validate a raw config dict against the appropriate pre-resolution schema.

    Args:
        raw: Raw config dictionary (variables not yet resolved)
        config_type: Type of config (thread, weave, loom, params, warp, project)
        at: Where ``raw`` sits in its file, for an inline weave

    Returns:
        Validated Pydantic model instance

    Raises:
        ConfigSchemaError: If validation fails
    """
    model_map: dict[str, type[BaseModel]] = {
        "thread": ThreadConfig,
        "weave": WeaveConfig,
        "loom": LoomConfig,
        "params": ParamsConfig,
        "warp": WarpConfig,
        "project": Project,
    }

    if config_type not in model_map:
        raise ConfigSchemaError.from_diagnostic(
            diagnostic(
                "C204",
                f"Unknown config type '{config_type}', expected one of: "
                f"{', '.join(model_map.keys())}",
                expected=list(model_map),
            )
        )

    model_class = model_map[config_type]
    removed = removed_key_diagnostics(raw, config_type, at=at)
    if config_type == "project":
        raw, reserved = split_reserved_blocks(raw)
        removed = [*removed, *reserved]

    try:
        validated = model_class.model_validate(raw)
    except ValidationError as exc:
        raise ConfigSchemaError(
            f"Schema validation failed for {config_type} config: {exc}",
            cause=exc,
            diagnostics=[*removed, *pointed(from_validation_error(exc, raw), at)],
        ) from exc
    if removed:
        raise ConfigSchemaError(
            f"{config_type} config uses keys this version does not read",
            diagnostics=removed,
        )
    return validated


def validate_inline_thread_types(entry: dict[str, Any], *, at: tuple[str | int, ...] = ()) -> None:
    """Check the value types of an inline thread body before the cascade reads it.

    Unlike :func:`validate_schema`, missing fields are not errors here: a
    name-only entry, or an inline body without its target, is reported by
    model hydration with the thread's name. Only values of the wrong type
    (``sources: [a]``, ``lookups: 5``) are rejected, because the cascade and
    the load checks read them before hydration.

    Args:
        entry: An inline thread entry from a weave's ``threads`` list.
        at: Where the entry sits in its file (``("threads", 2)``).

    Raises:
        ConfigSchemaError: If a typed field holds a value of the wrong type.
    """
    removed = removed_key_diagnostics(entry, "thread", at=at)
    try:
        ThreadConfig.model_validate(entry)
    except ValidationError as exc:
        wrong = [error for error in exc.errors() if error["type"] != "missing"]
        if wrong:
            details = "; ".join(
                f"{'.'.join(str(part) for part in error['loc'])}: {error['msg']}" for error in wrong
            )
            name = entry.get("name", "")
            wrong_type = [d for d in from_validation_error(exc, entry) if d.code != "C101"]
            diagnostics = [*removed, *pointed(wrong_type, at)]
            raise ConfigSchemaError(
                f"Schema validation failed for inline thread '{name}': {details}",
                cause=exc,
                diagnostics=diagnostics,
            ) from exc
    if removed:
        raise ConfigSchemaError(
            f"Inline thread '{entry.get('name', '')}' uses keys this version no longer reads",
            diagnostics=removed,
        )


# ---------------------------------------------------------------------------
# Rendering and grouping coded diagnostics
# ---------------------------------------------------------------------------


def render_diagnostics(diagnostics: Sequence[Diagnostic]) -> list[str]:
    """Render diagnostics as the ``"ERROR: …"`` / ``"WARN: …"`` strings validators return.

    Args:
        diagnostics: Coded diagnostics; info diagnostics are left out.

    Returns:
        One prefixed string per error or warning, in order.
    """
    rendered: list[str] = []
    for d in diagnostics:
        if d.severity is Severity.ERROR:
            rendered.append(f"ERROR: {d.message}")
        elif d.severity is Severity.WARNING:
            rendered.append(f"WARN: {d.message}")
    return rendered


def partition_diagnostics(
    diagnostics: Sequence[Diagnostic],
) -> tuple[list[Diagnostic], list[Diagnostic]]:
    """Split diagnostics into ``(errors, warnings)``; info diagnostics are dropped."""
    errors = [d for d in diagnostics if d.severity is Severity.ERROR]
    warnings = [d for d in diagnostics if d.severity is Severity.WARNING]
    return errors, warnings


def in_thread(
    diagnostics: Sequence[Diagnostic],
    name: str,
    *,
    file: str | None = None,
    prefix: bool = True,
) -> list[Diagnostic]:
    """Attribute diagnostics to a thread.

    Args:
        diagnostics: Diagnostics from a check run on one thread.
        name: The thread's effective name; becomes each diagnostic's ``node``.
        file: The file the thread is written in, set where a diagnostic names none.
        prefix: Lead each message with ``thread '<name>': ``.

    Returns:
        The attributed diagnostics.
    """
    result: list[Diagnostic] = []
    for d in diagnostics:
        updated = replace(
            d,
            node=d.node or name,
            message=f"thread '{name}': {d.message}" if prefix else d.message,
        )
        result.append(updated.located(file) if file is not None else updated)
    return result


# ---------------------------------------------------------------------------
# Post-resolution cross-cutting validators for incremental processing
# ---------------------------------------------------------------------------

_PARAM_REF_PATTERN = re.compile(r"\$\{param\.")


def _contains_param_reference(obj: Any) -> bool:
    """Recursively check if an object tree contains ``${param.`` references."""
    if isinstance(obj, str):
        return bool(_PARAM_REF_PATTERN.search(obj))
    if isinstance(obj, dict):
        return any(_contains_param_reference(v) for v in obj.values())
    if isinstance(obj, list):
        return any(_contains_param_reference(item) for item in obj)
    return False


def incremental_config_diagnostics(thread_config: dict[str, Any]) -> list[Diagnostic]:
    """Check incremental processing cross-cutting constraints, as coded diagnostics.

    Called after variable resolution on the full thread config dict.

    Rules:
    - ``incremental_parameter`` mode should reference at least one ``${param.*}``
      variable in steps or sources (warning ``C483`` if missing).
    - ``cdc`` mode requires ``write.mode: merge`` (``C480``).
    - ``cdc`` mode with an explicit mapping declaring ``update_value`` or
      ``delete_value`` requires ``watermark_column`` — same-key changes have
      no defined order without it (``C481``). Insert-only mappings and the
      ``delta_cdf`` preset are exempt.
    - ``incremental_watermark`` mode with an effective ``write.mode: overwrite``
      (explicit or defaulted) would replace the target with each incremental
      slice (``C482``).
    - ``watermark_inclusive=True`` with ``write.mode: append`` is risky
      (warning ``C484``).

    Args:
        thread_config: The resolved thread config dict.

    Returns:
        Every rule that failed, in rule order.
    """
    found: list[Diagnostic] = []

    load = thread_config.get("load")
    if not load:
        return found

    mode = load.get("mode", "full")
    write = thread_config.get("write", {}) or {}
    write_mode = write.get("mode", "overwrite")
    mode_pointer = "/load/mode"

    if mode == "incremental_parameter":
        # Scan steps + sources for ${param.} references
        has_param = _contains_param_reference(
            thread_config.get("steps", [])
        ) or _contains_param_reference(thread_config.get("sources", {}))
        if not has_param:
            # Also check serialized config as string fallback
            config_str = json.dumps(thread_config)
            if "${param." not in config_str:
                found.append(
                    diagnostic(
                        "C483",
                        "incremental_parameter mode but no ${param.*} "
                        "references found in steps or sources",
                        resolved_path=mode_pointer,
                    )
                )

    if mode == "cdc" and write_mode != "merge":
        found.append(
            diagnostic(
                "C480",
                f"cdc mode requires write.mode='merge', got '{write_mode}'",
                resolved_path="/write/mode",
                expected=["merge"],
            )
        )

    if mode == "cdc":
        cdc = load.get("cdc") or {}
        is_generic = cdc.get("operation_column") is not None
        has_collision_ops = cdc.get("update_value") or cdc.get("delete_value")
        if is_generic and has_collision_ops and not load.get("watermark_column"):
            found.append(
                diagnostic(
                    "C481",
                    "cdc mode with update_value/delete_value requires "
                    "load.watermark_column to order same-key changes",
                    resolved_path="/load/watermark_column",
                )
            )

    if mode == "incremental_watermark" and write_mode == "overwrite":
        provenance = "" if "mode" in write else " (the default)"
        found.append(
            diagnostic(
                "C482",
                "incremental_watermark mode requires write.mode='append' or "
                f"'merge', got 'overwrite'{provenance}",
                resolved_path="/write/mode",
                expected=["append", "merge"],
            )
        )

    if load.get("watermark_inclusive") and write_mode == "append":
        found.append(
            diagnostic(
                "C484",
                "watermark_inclusive=True with write.mode='append' "
                "may cause duplicate rows on re-reads; prefer merge or overwrite",
                resolved_path="/load/watermark_inclusive",
            )
        )

    return found


def validate_incremental_config(thread_config: dict[str, Any]) -> list[str]:
    """Validate incremental processing cross-cutting constraints.

    Called after variable resolution on the full thread config dict.

    Returns a list of diagnostic messages. Messages starting with
    ``"ERROR:"`` are fatal; ``"WARN:"`` are advisory. See
    :func:`incremental_config_diagnostics` for the rules and their codes.
    """
    return render_diagnostics(incremental_config_diagnostics(thread_config))


def _incremental_input(thread: Thread, authored: dict[str, Any] | None) -> dict[str, Any]:
    """Build the incremental check's input from a hydrated thread."""
    load = thread.load
    load_raw: dict[str, Any] = {"mode": load.mode if load is not None else "full"}
    if load is not None:
        load_raw["watermark_inclusive"] = load.watermark_inclusive
        if load.watermark_column is not None:
            load_raw["watermark_column"] = load.watermark_column
        if load.cdc is not None:
            load_raw["cdc"] = load.cdc.model_dump()
    config: dict[str, Any] = {"load": load_raw}
    if thread.write is not None:
        config["write"] = {"mode": thread.write.mode}
    written = authored or {}
    config["steps"] = written.get("steps", [])
    config["sources"] = written.get("sources", {})
    return config


def incremental_thread_diagnostics(
    thread: Thread, authored: dict[str, Any] | None = None
) -> list[Diagnostic]:
    """Run :func:`incremental_config_diagnostics` on a hydrated thread.

    The load and the engine build the check's input the same way: load
    and write settings from the hydrated (cascaded) thread, steps and
    sources from ``authored``, the thread as written. The ``${param.*}``
    rule needs the written form: after substitution a parameterized filter
    looks like a literal one. Without ``authored`` that rule always warns.

    Args:
        thread: The hydrated thread.
        authored: The thread's config dict before variable substitution.

    Returns:
        The diagnostics, errors and warnings.
    """
    return incremental_config_diagnostics(_incremental_input(thread, authored))


def incremental_diagnostics(thread: Thread, authored: dict[str, Any] | None = None) -> list[str]:
    """Run :func:`validate_incremental_config` on a hydrated thread.

    See :func:`incremental_thread_diagnostics` for how the input is built.

    Args:
        thread: The hydrated thread.
        authored: The thread's config dict before variable substitution.

    Returns:
        The diagnostics, ``"ERROR:"`` or ``"WARN:"`` prefixed.
    """
    return render_diagnostics(incremental_thread_diagnostics(thread, authored))


# ---------------------------------------------------------------------------
# Post-resolution cross-cutting validators for dimension mode
# ---------------------------------------------------------------------------

#: write fields that the engine derives automatically from the dimension block
_DIMENSION_FORBIDDEN_WRITE_FIELDS = ("match_keys", "on_match")

#: keys fields declared inside the dimension block; forbidden at top level
_DIMENSION_FORBIDDEN_KEYS_FIELDS = ("business_key", "surrogate_key", "change_detection")


def dimension_override_diagnostics(thread_config: dict[str, Any]) -> list[Diagnostic]:
    """Check write/keys override constraints when a dimension block is present.

    Called after variable resolution on the full thread config dict.

    Rules (only applied when ``target.dimension`` is present):

    - ``write.match_keys`` and ``write.on_match`` → ``C485`` (engine derives them)
    - ``write.mode`` → ``C486`` (dimension targets always merge)
    - ``keys.business_key``, ``keys.surrogate_key`` and ``keys.change_detection``
      → ``C487`` (declared in the dimension block)

    Each diagnostic's ``resolved_path`` is the offending field, relative to the
    thread.

    Args:
        thread_config: The thread config dict.

    Returns:
        Every forbidden field found, in rule order.
    """
    found: list[Diagnostic] = []

    target = thread_config.get("target") or {}
    if "dimension" not in target:
        return found

    write = thread_config.get("write") or {}
    keys = thread_config.get("keys") or {}

    for field in _DIMENSION_FORBIDDEN_WRITE_FIELDS:
        if field in write:
            found.append(
                diagnostic(
                    "C485",
                    f"write.{field} cannot be set when target.dimension is present "
                    f"(the engine derives it from the dimension block)",
                    resolved_path=f"/write/{field}",
                )
            )

    if "mode" in write:
        found.append(
            diagnostic(
                "C486",
                f"write.mode '{write['mode']}' cannot be set when target.dimension is "
                f"present (dimension targets always merge); remove write.mode",
                resolved_path="/write/mode",
            )
        )

    for field in _DIMENSION_FORBIDDEN_KEYS_FIELDS:
        if field in keys:
            found.append(
                diagnostic(
                    "C487",
                    f"keys.{field} cannot be set when target.dimension is present "
                    f"(declare it inside the dimension block instead)",
                    resolved_path=f"/keys/{field}",
                )
            )

    return found


def validate_dimension_overrides(thread_config: dict[str, Any]) -> list[str]:
    """Validate write/keys override constraints when a dimension block is present.

    Called after variable resolution on the full thread config dict.

    Returns a list of diagnostic messages, each ``"ERROR:"`` prefixed. See
    :func:`dimension_override_diagnostics` for the rules and their codes.
    """
    return render_diagnostics(dimension_override_diagnostics(thread_config))


# ---------------------------------------------------------------------------
# Post-resolution cross-cutting validators for lookup and column-set references
# ---------------------------------------------------------------------------


def resolve_lookup_diagnostics(
    thread_config: dict[str, Any],
    *,
    available_lookups: set[str],
) -> list[Diagnostic]:
    """Check that resolve steps reference lookup names the thread can see.

    Called during the shared config load on each resolved thread dict, after
    ``foreach`` expansion. Covers the main pipeline and every ``with:``
    sub-pipeline.

    Args:
        thread_config: Resolved thread configuration dictionary.
        available_lookups: Names a resolve step can use: the thread's,
            weave's, and loom's lookups plus the thread's lookup-bound
            source aliases.

    Returns:
        One diagnostic per undefined name: ``R120`` (error), or ``R121``
        (warning) for a single-mode step with ``on_failure: warn``, which
        runs on and assigns ``on_unknown`` to every row. Each carries the
        available names as ``expected``.
    """
    found: list[Diagnostic] = []
    steps = list(thread_config.get("steps") or [])
    # Resolved dicts carry the block as ``with_`` (the model field); raw YAML as ``with``
    sub_pipelines = thread_config.get("with_") or thread_config.get("with") or {}
    if isinstance(sub_pipelines, dict):
        for sub in sub_pipelines.values():
            if isinstance(sub, dict) and isinstance(sub.get("steps"), list):
                steps.extend(sub["steps"])

    for step in steps:
        if not isinstance(step, dict) or "resolve" not in step:
            continue

        resolve = step["resolve"]
        if not isinstance(resolve, dict):
            continue

        # (name, code): a single-mode warn step runs on; batch always aborts
        references: list[tuple[str, str]] = []
        if "lookup" in resolve and isinstance(resolve["lookup"], str):
            code = "R121" if resolve.get("on_failure") == "warn" else "R120"
            references.append((resolve["lookup"], code))
        batch = resolve.get("batch")
        if isinstance(batch, list):
            for item in batch:
                if isinstance(item, dict) and "lookup" in item:
                    references.append((item["lookup"], "R120"))

        for name, code in references:
            if name not in available_lookups:
                found.append(
                    diagnostic(
                        code,
                        f"resolve step references undefined lookup "
                        f"'{name}'; available lookups: "
                        f"{sorted(available_lookups) or '(none)'}",
                        step_type="resolve",
                        expected=sorted(available_lookups),
                    )
                )

    return found


def validate_resolve_lookups(
    thread_config: dict[str, Any],
    *,
    available_lookups: set[str],
) -> list[str]:
    """Validate that resolve steps reference lookup names the thread can see.

    Args:
        thread_config: Resolved thread configuration dictionary.
        available_lookups: Names a resolve step can use.

    Returns:
        List of diagnostic messages. ``"ERROR:"`` prefixed entries are
        fatal; a single-mode step with ``on_failure: warn`` gets a ``"WARN:"``
        instead. See :func:`resolve_lookup_diagnostics`.
    """
    return render_diagnostics(
        resolve_lookup_diagnostics(thread_config, available_lookups=available_lookups)
    )


def rename_column_set_diagnostics(
    thread_config: dict[str, Any],
    *,
    available_column_sets: set[str],
    standalone: bool = False,
) -> list[Diagnostic]:
    """Check that rename steps name column sets the thread can see.

    Covers the main pipeline and every ``with:`` sub-pipeline. A set that
    is defined but resolves to no mapping at run time (an optional
    param-sourced set, say) is fine here; only a name no scope defines is
    reported.

    Args:
        thread_config: Resolved thread configuration dictionary.
        available_column_sets: Names defined by the thread and, unless
            ``standalone``, its weave and loom.
        standalone: The thread was loaded from its own file, so a weave or
            loom that defines the set can't be seen. Missing names warn
            instead of failing, since such a thread is valid in its weave.

    Returns:
        One diagnostic per undefined name: ``R124`` (error) or, when
        ``standalone``, ``R125`` (warning). Each carries the step's
        zero-based ``step_index`` within its pipeline and ``step_type``
        ``rename``; errors also carry the available names as ``expected``.
    """
    pipelines: list[tuple[str, Any]] = [("", thread_config.get("steps"))]
    # Resolved dicts carry the block as ``with_`` (the model field); raw YAML as ``with``
    sub_pipelines = thread_config.get("with_") or thread_config.get("with") or {}
    if isinstance(sub_pipelines, dict):
        for sub_name, sub in sub_pipelines.items():
            if isinstance(sub, dict):
                pipelines.append((f"with.{sub_name} ", sub.get("steps")))

    found: list[Diagnostic] = []
    available = sorted(available_column_sets)
    for prefix, steps in pipelines:
        if not isinstance(steps, list):
            continue
        for index, step in enumerate(steps, start=1):
            rename = step.get("rename") if isinstance(step, dict) else None
            name = rename.get("column_set") if isinstance(rename, dict) else None
            if not isinstance(name, str) or name in available_column_sets:
                continue
            where = f"{prefix}step {index}"
            if standalone:
                found.append(
                    diagnostic(
                        "R125",
                        f"rename {where} references column set '{name}', which this "
                        f"thread doesn't define; it must come from a weave or loom",
                        step_index=index - 1,
                        step_type="rename",
                    )
                )
            else:
                found.append(
                    diagnostic(
                        "R124",
                        f"rename {where} references undefined column set '{name}'; "
                        f"available column sets: {available or '(none)'}",
                        step_index=index - 1,
                        step_type="rename",
                        expected=available,
                    )
                )
    return found


def validate_rename_column_sets(
    thread_config: dict[str, Any],
    *,
    available_column_sets: set[str],
    standalone: bool = False,
) -> list[str]:
    """Validate that rename steps name column sets the thread can see.

    Args:
        thread_config: Resolved thread configuration dictionary.
        available_column_sets: Names defined by the thread and, unless
            ``standalone``, its weave and loom.
        standalone: The thread was loaded from its own file; missing names
            warn instead of failing.

    Returns:
        List of diagnostic messages, ``"ERROR:"`` or ``"WARN:"`` prefixed.
        See :func:`rename_column_set_diagnostics`.
    """
    return render_diagnostics(
        rename_column_set_diagnostics(
            thread_config, available_column_sets=available_column_sets, standalone=standalone
        )
    )


def source_lookup_diagnostics(
    thread_config: dict[str, Any],
    *,
    available_lookups: set[str],
    standalone: bool = False,
) -> list[Diagnostic]:
    """Check that lookup-bound sources name lookups the thread can see.

    Args:
        thread_config: Resolved thread configuration dictionary.
        available_lookups: Names defined by the thread and, unless
            ``standalone``, its weave and loom.
        standalone: The thread was loaded from its own file, so a weave or
            loom that defines the lookup can't be seen. Missing names warn
            instead of failing, since such a thread is valid in its weave.

    Returns:
        One diagnostic per undefined name: ``R122`` (error) or, when
        ``standalone``, ``R123`` (warning). Errors carry the available names
        as ``expected``.
    """
    found: list[Diagnostic] = []
    sources = thread_config.get("sources") or {}
    available = sorted(available_lookups)
    for alias, source in sources.items():
        name = source.get("lookup") if isinstance(source, dict) else None
        if name is None or name in available_lookups:
            continue
        if standalone:
            found.append(
                diagnostic(
                    "R123",
                    f"source '{alias}' references lookup '{name}', which this thread "
                    f"doesn't define; it must come from a weave or loom",
                    resolved_path=json_pointer(("sources", alias, "lookup")),
                )
            )
        else:
            found.append(
                diagnostic(
                    "R122",
                    f"source '{alias}' references undefined lookup '{name}'; "
                    f"available lookups: {available or '(none)'}",
                    resolved_path=json_pointer(("sources", alias, "lookup")),
                    expected=available,
                )
            )
    return found


def validate_source_lookups(
    thread_config: dict[str, Any],
    *,
    available_lookups: set[str],
    standalone: bool = False,
) -> list[str]:
    """Validate that lookup-bound sources name lookups the thread can see.

    Args:
        thread_config: Resolved thread configuration dictionary.
        available_lookups: Names defined by the thread and, unless
            ``standalone``, its weave and loom.
        standalone: The thread was loaded from its own file; missing names
            warn instead of failing.

    Returns:
        List of diagnostic messages, ``"ERROR:"`` or ``"WARN:"`` prefixed.
        See :func:`source_lookup_diagnostics`.
    """
    return render_diagnostics(
        source_lookup_diagnostics(
            thread_config, available_lookups=available_lookups, standalone=standalone
        )
    )


def split_diagnostics(diagnostics: list[str]) -> tuple[list[str], list[str]]:
    """Split validator diagnostics into ``(errors, warnings)``, prefixes removed.

    Validators report ``"ERROR: …"`` (fatal) and ``"WARN: …"`` (advisory)
    strings; anything else is ignored.
    """
    errors = [d.removeprefix("ERROR:").lstrip() for d in diagnostics if d.startswith("ERROR:")]
    warnings = [d.removeprefix("WARN:").lstrip() for d in diagnostics if d.startswith("WARN:")]
    return errors, warnings


def execution_scope_diagnostics(
    defaults_by_scope: list[tuple[str, dict[str, Any] | None]],
    threads: list[dict[str, Any]],
) -> list[Diagnostic]:
    """Check for execution settings that will not apply.

    Thread-scoped ``execution:`` blocks are declared but uniformly not
    applied in v1.x, whether authored on the thread or arrived via a
    ``defaults.execution`` cascade. Diagnostics are deduplicated per origin:
    one (``C490``) per scope whose ``defaults`` carries an ``execution``
    block, and one (``C491``) per thread that authored its own block.

    Args:
        defaults_by_scope: ``(scope label, defaults dict)`` pairs for each
            enclosing scope (e.g. ``("loom 'nightly'", {...})``).
        threads: Raw thread config dicts *before* the inheritance cascade,
            so an ``execution`` key means the thread authored it.

    Returns:
        Warning diagnostics, one per distinct origin.
    """
    found: list[Diagnostic] = []

    for scope_label, defaults in defaults_by_scope:
        if defaults and isinstance(defaults.get("execution"), dict):
            fields = ", ".join(sorted(defaults["execution"]))
            found.append(
                diagnostic(
                    "C490",
                    f"{scope_label} declares defaults.execution ({fields}) — "
                    f"thread-scoped execution settings are not applied. Move them "
                    f"to the top-level execution block on the loom or weave.",
                    hint="Move the settings to the top-level execution block.",
                )
            )

    for i, thread in enumerate(threads):
        execution = thread.get("execution")
        if isinstance(execution, dict) and execution:
            name = thread.get("name") or f"#{i}"
            fields = ", ".join(sorted(execution))
            found.append(
                diagnostic(
                    "C491",
                    f"thread '{name}' declares an execution block ({fields}) — "
                    f"thread-scoped execution settings are not applied. Move them "
                    f"to the top-level execution block on the loom or weave.",
                    node=thread.get("name") or None,
                    resolved_path="/execution",
                    hint="Move the settings to the top-level execution block.",
                )
            )

    return found


def collect_execution_scope_warnings(
    defaults_by_scope: list[tuple[str, dict[str, Any] | None]],
    threads: list[dict[str, Any]],
) -> list[str]:
    """Collect warnings for execution settings that will not apply.

    Args:
        defaults_by_scope: ``(scope label, defaults dict)`` pairs for each
            enclosing scope.
        threads: Raw thread config dicts *before* the inheritance cascade.

    Returns:
        ``"WARN:"`` strings, one per distinct origin. See
        :func:`execution_scope_diagnostics`.
    """
    return render_diagnostics(execution_scope_diagnostics(defaults_by_scope, threads))


def trace_composition_diagnostics(
    loom_execution: Any,
    weave_executions: dict[str, Any],
) -> list[Diagnostic]:
    """Warn (``C492``) when a weave's explicit ``trace: true`` cannot re-enable tracing.

    The root span collector exists (or not) for the whole run: under a loom
    whose effective ``trace`` is false there is nothing for a weave to
    attach to, so an explicit ``trace: true`` on the weave is a no-op.

    Args:
        loom_execution: The loom's hydrated ``ExecutionConfig`` (or None).
        weave_executions: Hydrated weave ``ExecutionConfig`` (or None) by
            weave name.

    Returns:
        Warning diagnostics, one per weave with an ineffective ``trace: true``.
    """
    if resolve_effective_execution(None, loom_execution).trace:
        return []

    found: list[Diagnostic] = []
    for weave_name, execution in weave_executions.items():
        if execution is not None and "trace" in execution.model_fields_set and execution.trace:
            found.append(
                diagnostic(
                    "C492",
                    f"weave '{weave_name}' sets trace: true, but the loom's "
                    f"effective trace is false — tracing cannot be re-enabled "
                    f"per-weave and no telemetry will be collected.",
                    node=weave_name,
                )
            )
    return found


def collect_trace_composition_warnings(
    loom_execution: Any,
    weave_executions: dict[str, Any],
) -> list[str]:
    """Warn when a weave's explicit ``trace: true`` cannot re-enable tracing.

    Args:
        loom_execution: The loom's hydrated ``ExecutionConfig`` (or None).
        weave_executions: Hydrated weave ``ExecutionConfig`` (or None) by
            weave name.

    Returns:
        ``"WARN:"`` strings, one per weave with an ineffective ``trace: true``.
        See :func:`trace_composition_diagnostics`.
    """
    return render_diagnostics(trace_composition_diagnostics(loom_execution, weave_executions))
