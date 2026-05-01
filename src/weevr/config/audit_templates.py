"""Pure-Python helpers for resolving and merging audit column templates.

This is the canonical home for the audit-template merge primitives used by
``weevr.config.inheritance``. The historical home in ``weevr.operations.audit``
re-exports these symbols for back-compat with external callers; core modules
(notably ``weevr.config.inheritance``) MUST import from here directly so the
config layer stays Spark-free.
"""

from __future__ import annotations

import fnmatch
import logging
import re
from dataclasses import dataclass

from weevr.errors.exceptions import ConfigError

logger = logging.getLogger(__name__)

__all__ = [
    "BUILTIN_AUDIT_PRESETS",
    "AuditContext",
    "CONTEXT_VAR_PATTERN",
    "resolve_audit_columns",
    "apply_audit_exclusions",
    "resolve_template_columns",
    "merge_audit_columns_with_templates",
    "resolve_context_variables",
]

# ---------------------------------------------------------------------------
# Built-in audit presets
# ---------------------------------------------------------------------------

#: Built-in audit column presets keyed by name.
#:
#: ``fabric`` — nine columns aligned with Microsoft Fabric pipeline parameters.
#: ``minimal`` — three lightweight columns for simple run tracking.
BUILTIN_AUDIT_PRESETS: dict[str, dict[str, str]] = {
    "fabric": {
        "_batch_id": "${param.batch_id}",
        "_batch_version": "${param.batch_version}",
        "_batch_source": "${param.batch_source}",
        "_batch_process_ts": "current_timestamp()",
        "_pipeline_id": "${param.pipeline_id}",
        "_pipeline_name": "${param.pipeline_name}",
        "_workspace_id": "${param.workspace_id}",
        "_spark_app_id": "spark_context().applicationId",
        "_task_ts": "current_timestamp()",
    },
    "minimal": {
        "_weevr_loaded_at": "current_timestamp()",
        "_weevr_run_id": "${param.run_id}",
        "_weevr_thread": "${thread.name}",
    },
}


@dataclass(frozen=True)
class AuditContext:
    """Execution context for resolving audit column variables.

    Attributes:
        thread_name: Thread name (e.g. ``stg_orders``).
        thread_qualified_key: Fully qualified key (e.g. ``staging.stg_orders``).
        thread_source: Primary source alias (first declared source).
        thread_sources_json: JSON array of all sources with type metadata.
        weave_name: Weave name (e.g. ``staging``).
        loom_name: Loom name (e.g. ``my-project``).
        run_timestamp: ISO 8601 UTC timestamp of execution start.
        run_id: UUID4 identifier unique per execution.
    """

    thread_name: str
    thread_qualified_key: str
    thread_source: str | None
    thread_sources_json: str
    weave_name: str
    loom_name: str
    run_timestamp: str = ""
    run_id: str = ""


# Pattern matching ${namespace.property} context variables.
CONTEXT_VAR_PATTERN = re.compile(r"\$\{(thread|weave|loom|run)\.([a-z_]+)\}")


def resolve_audit_columns(
    loom_audit: dict[str, str] | None,
    weave_audit: dict[str, str] | None,
    thread_audit: dict[str, str] | None,
) -> dict[str, str]:
    """Merge audit columns additively: loom -> weave -> thread.

    Lower levels extend the column set. Same-named columns at a lower
    level override the expression from a higher level.

    Args:
        loom_audit: Audit columns from loom defaults.
        weave_audit: Audit columns from weave defaults.
        thread_audit: Audit columns from thread target.

    Returns:
        Merged audit column dict. Empty dict if none defined.
    """
    merged: dict[str, str] = {}
    if loom_audit:
        merged.update(loom_audit)
    if weave_audit:
        merged.update(weave_audit)
    if thread_audit:
        merged.update(thread_audit)
    return merged


def apply_audit_exclusions(
    columns: dict[str, str],
    exclude_patterns: list[str] | None,
) -> dict[str, str]:
    """Remove audit columns whose names match any of the given glob patterns.

    Args:
        columns: Resolved audit column definitions as ``{name: expression}``.
        exclude_patterns: List of glob patterns (``fnmatch`` syntax). ``None``
            or an empty list is a no-op.

    Returns:
        Copy of ``columns`` with matching entries removed.
    """
    if not exclude_patterns:
        return columns

    result: dict[str, str] = {}
    for name, expression in columns.items():
        matched = False
        for pattern in exclude_patterns:
            if fnmatch.fnmatch(name, pattern):
                logger.debug("Audit column %r excluded by pattern %r", name, pattern)
                matched = True
                break
        if not matched:
            result[name] = expression

    return result


def resolve_template_columns(
    template_names: list[str],
    user_templates: dict[str, dict[str, str]] | None = None,
) -> dict[str, str]:
    """Resolve template names to merged column definitions.

    Looks up each name in user-defined templates first, then built-in
    presets. Logs a warning when a user-defined template shadows a
    built-in preset. Raises ConfigError if a name is not found.

    Args:
        template_names: Ordered list of template names to resolve.
        user_templates: User-defined templates keyed by name.

    Returns:
        Merged column dict. Later names override earlier on collision.

    Raises:
        ConfigError: If a template name is not found in user-defined
            or built-in presets.
    """
    resolved_user = user_templates or {}
    merged: dict[str, str] = {}

    for name in template_names:
        if name in resolved_user:
            if name in BUILTIN_AUDIT_PRESETS:
                logger.warning(
                    "User-defined template %r shadows built-in preset with the same name",
                    name,
                )
            merged.update(resolved_user[name])
        elif name in BUILTIN_AUDIT_PRESETS:
            merged.update(BUILTIN_AUDIT_PRESETS[name])
        else:
            available = sorted(set(resolved_user) | set(BUILTIN_AUDIT_PRESETS))
            raise ConfigError(
                f"Audit template {name!r} not found. Available templates: {', '.join(available)}"
            )

    return merged


def merge_audit_columns_with_templates(
    *,
    loom_templates: dict[str, dict[str, str]] | None = None,
    loom_template_refs: list[str] | None = None,
    loom_inline: dict[str, str] | None = None,
    weave_templates: dict[str, dict[str, str]] | None = None,
    weave_template_refs: list[str] | None = None,
    weave_inline: dict[str, str] | None = None,
    thread_templates: dict[str, dict[str, str]] | None = None,
    thread_template_refs: list[str] | None = None,
    thread_inline: dict[str, str] | None = None,
    thread_inherit: bool = True,
    thread_exclude: list[str] | None = None,
) -> dict[str, str]:
    """Merge audit columns from templates, inline definitions, and inheritance.

    Resolution order:
    1. Collect user-defined templates from all levels.
    2. If thread_inherit is False, skip loom and weave template refs and inline.
    3. Resolve template refs at each level via resolve_template_columns().
    4. Merge: loom template cols → loom inline → weave template cols
       → weave inline → thread template cols → thread inline.
    5. Apply apply_audit_exclusions() with thread_exclude.

    User-defined template definitions from all levels are always available for
    name resolution, even when thread_inherit is False. Inheritance only controls
    whether loom and weave template refs and inline columns contribute to the
    merged output.

    Args:
        loom_templates: User-defined templates declared at the loom level.
        loom_template_refs: Template names to resolve at the loom level.
        loom_inline: Explicit audit columns defined inline at the loom level.
        weave_templates: User-defined templates declared at the weave level.
        weave_template_refs: Template names to resolve at the weave level.
        weave_inline: Explicit audit columns defined inline at the weave level.
        thread_templates: User-defined templates declared at the thread level.
        thread_template_refs: Template names to resolve at the thread level.
        thread_inline: Explicit audit columns defined inline at the thread level.
        thread_inherit: When False, loom and weave template refs and inline
            columns are excluded from the merged result. Defaults to True.
        thread_exclude: Glob patterns applied after all merging. Matching
            column names are removed from the final result.

    Returns:
        Merged audit column dict. Empty dict if nothing is defined.
    """
    # Merge all user-defined templates across levels so they are available for
    # any level's template refs regardless of inheritance setting.
    all_user_templates: dict[str, dict[str, str]] = {}
    if loom_templates:
        all_user_templates.update(loom_templates)
    if weave_templates:
        all_user_templates.update(weave_templates)
    if thread_templates:
        all_user_templates.update(thread_templates)

    merged: dict[str, str] = {}

    if thread_inherit:
        if loom_template_refs:
            merged.update(resolve_template_columns(loom_template_refs, all_user_templates))
        if loom_inline:
            merged.update(loom_inline)
        if weave_template_refs:
            merged.update(resolve_template_columns(weave_template_refs, all_user_templates))
        if weave_inline:
            merged.update(weave_inline)

    if thread_template_refs:
        merged.update(resolve_template_columns(thread_template_refs, all_user_templates))
    if thread_inline:
        merged.update(thread_inline)

    return apply_audit_exclusions(merged, thread_exclude)


def resolve_context_variables(expression: str, context: AuditContext) -> str:
    """Substitute ${namespace.property} variables in an expression string.

    Args:
        expression: Spark SQL expression with context variable placeholders.
        context: Audit context providing variable values.

    Returns:
        Expression with context variables replaced by their values.
    """
    lookup: dict[str, dict[str, str | None]] = {
        "thread": {
            "name": context.thread_name,
            "qualified_key": context.thread_qualified_key,
            "source": context.thread_source,
            "sources": context.thread_sources_json,
        },
        "weave": {
            "name": context.weave_name,
        },
        "loom": {
            "name": context.loom_name,
        },
        "run": {
            "timestamp": context.run_timestamp,
            "id": context.run_id,
        },
    }

    def _replace(match: re.Match[str]) -> str:
        namespace = match.group(1)
        prop = match.group(2)
        ns_lookup = lookup.get(namespace, {})
        if prop not in ns_lookup:
            return match.group(0)  # Unknown property — leave unresolved
        value = ns_lookup[prop]
        if value is None:
            logger.warning(
                "Audit column context variable ${%s.%s} resolved to None, "
                "substituting empty string",
                namespace,
                prop,
            )
            return ""
        return value

    return CONTEXT_VAR_PATTERN.sub(_replace, expression)
