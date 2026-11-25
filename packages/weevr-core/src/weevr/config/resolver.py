"""Variable interpolation and reference resolution."""

import re
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any

from weevr.config.collect import skip_on_error
from weevr.config.interpolation import build_param_context
from weevr.config.locations import ConfigLocation
from weevr.config.params import (
    BindingReads,
    ParamScope,
    ScopePositions,
    missing_parameter,
    spec_field,
)
from weevr.config.parser import TYPED_EXTENSIONS
from weevr.config.scoping import (
    enter_child,
    is_inline_thread,
    is_inline_weave,
    resolve_scoped,
    substitute,
)
from weevr.diagnostics import Diagnostic, diagnostic
from weevr.diagnostics.positions import json_pointer
from weevr.errors import (
    ConfigError,
    ConfigSchemaError,
    ReferenceResolutionError,
)


@dataclass
class ResolvedChild:
    """A child config pulled in by one entry of its weave or loom.

    Reference resolution returns these instead of attaching children to the
    parent's dict; the cascade, the pre-cascade checks and hydration walk them.

    Attributes:
        entry: The parent's entry for this child, as written (a bare string
            entry becomes ``{"name": ...}``).
        data: The child's resolved config dict. The inheritance cascade
            replaces it with the cascaded dict.
        source_path: The child's own file, or ``None`` for an inline child,
            which lives in its parent's file.
        authored: The child's config before variable substitution.
        children: A weave's threads (for a loom's weave children).
        at: Where an inline child sits in its parent's file
            (``("threads", 2)``); empty for a child in its own file.
    """

    entry: dict[str, Any]
    data: dict[str, Any]
    source_path: str | None = None
    authored: dict[str, Any] | None = None
    children: list["ResolvedChild"] = field(default_factory=list)
    at: tuple[str | int, ...] = ()


def resolve_declared_params(
    param_specs: dict[str, Any] | None,
    runtime_params: dict[str, Any] | None,
    *,
    file_path: str | None = None,
) -> dict[str, Any]:
    """Resolve loom/weave-level declared ``params:`` to a flat ``{name: value}`` dict.

    Precedence per declared param:

    1. Value from ``runtime_params`` if supplied
    2. ``ParamSpec.default`` if set on the spec
    3. ``ConfigSchemaError`` if the param is required
    4. Omitted from the result if optional with no default

    The returned dict is intended to bind under the ``param.*`` namespace via
    :func:`build_param_context`'s ``entry_params`` argument, so that
    ``${param.x}`` expressions resolve to the layered value.

    Runtime keys not declared in ``param_specs`` are ignored — declared scope
    is the only contract honored at this layer.

    Args:
        param_specs: Mapping of declared params (``ParamSpec`` instances or
            their ``model_dump`` dicts). ``None`` or empty returns ``{}``.
        runtime_params: Caller-supplied values keyed by param name.
        file_path: Optional originating config file path for error messages.

    Returns:
        Resolved ``{name: value}`` dict ready for ``entry_params``.

    Raises:
        ConfigSchemaError: A required declared param has no runtime value and
            no spec default. The message includes the file path and param name.
    """
    if not param_specs:
        return {}

    for name, spec in param_specs.items():
        if not isinstance(spec, dict) and not hasattr(spec, "required"):
            message = (
                f"Parameter '{name}' must be a mapping with 'name' and 'type', "
                f"got {type(spec).__name__}"
            )
            raise ConfigSchemaError(
                message,
                file_path=file_path,
                config_key=name,
                diagnostics=[
                    diagnostic(
                        "C102",
                        message,
                        file=file_path,
                        pointer=json_pointer(("params", name)),
                        expected=["mapping"],
                    )
                ],
            )

    runtime = runtime_params or {}
    resolved: dict[str, Any] = {}
    for name, spec in param_specs.items():
        # Treat an explicit None runtime value as absent so that the declared
        # default and required-check still apply. A literal None almost always
        # signals an unbound notebook variable rather than an intentional null.
        if name in runtime and runtime[name] is not None:
            resolved[name] = runtime[name]
            continue

        default = spec_field(spec, "default", default=None)
        if default is not None:
            resolved[name] = default
            continue

        required = spec_field(spec, "required", default=True)
        if required:
            message = f"Required parameter '{name}' is missing (no runtime value and no default)"
            raise ConfigSchemaError(
                message,
                file_path=file_path,
                config_key=name,
                diagnostics=[missing_parameter(name, message, file_path)],
            )
        # Optional with no default → omit
    return resolved


def _name_only_entry(kind: str, name: str, pointer: str) -> ReferenceResolutionError:
    """A weave or thread entry that names a child but has neither ``ref`` nor a body."""
    return _reference_error(
        "C313",
        f"{kind.capitalize()} entry '{name}' has a name but no body",
        pointer=pointer,
        hint=f"ref: {name}.{kind}",
    )


def validate_params(
    param_specs: dict[str, Any] | None,
    context: dict[str, Any],
) -> None:
    """Validate parameters against their type specifications.

    Args:
        param_specs: Parameter specifications from config
        context: Actual parameter values

    Raises:
        ConfigSchemaError: If required params missing or type mismatches
    """
    if not param_specs:
        return

    for param_name, spec in param_specs.items():
        # Handle both dict specs and ParamSpec objects
        if hasattr(spec, "required"):
            required = spec.required
            param_type = spec.type
            default = spec.default
        else:
            required = spec.get("required", True)
            param_type = spec.get("type", "string")
            default = spec.get("default")

        # Check required params
        if required and param_name not in context:
            if default is not None:
                # Use default if provided
                context[param_name] = default
                continue
            else:
                message = f"Required parameter '{param_name}' is missing"
                raise ConfigSchemaError(
                    message,
                    config_key=param_name,
                    diagnostics=[missing_parameter(param_name, message, None)],
                )

        # Skip validation if param not present (and not required)
        if param_name not in context:
            continue

        value = context[param_name]

        # Type validation
        if param_type == "string":
            if not isinstance(value, str):
                raise _parameter_error(
                    "P102",
                    param_name,
                    f"Parameter '{param_name}' expected string, got {type(value).__name__}",
                )

        elif param_type == "int":
            if not isinstance(value, int) or isinstance(value, bool):
                raise _parameter_error(
                    "P102",
                    param_name,
                    f"Parameter '{param_name}' expected int, got {type(value).__name__}",
                )

        elif param_type == "float":
            if not isinstance(value, (int, float)) or isinstance(value, bool):
                raise _parameter_error(
                    "P102",
                    param_name,
                    f"Parameter '{param_name}' expected float, got {type(value).__name__}",
                )

        elif param_type == "bool":
            if not isinstance(value, bool):
                raise _parameter_error(
                    "P102",
                    param_name,
                    f"Parameter '{param_name}' expected bool, got {type(value).__name__}",
                )

        elif param_type == "date":
            if isinstance(value, str):
                try:
                    datetime.strptime(value, "%Y-%m-%d")
                except ValueError as e:
                    raise _parameter_error(
                        "P103",
                        param_name,
                        f"Parameter '{param_name}' invalid date format, expected YYYY-MM-DD",
                        cause=e,
                    ) from e
            elif not isinstance(value, date):
                raise _parameter_error(
                    "P102",
                    param_name,
                    f"Parameter '{param_name}' expected date or ISO date string",
                )

        elif param_type == "timestamp":
            if isinstance(value, str):
                try:
                    datetime.fromisoformat(value)
                except ValueError as e:
                    raise _parameter_error(
                        "P103",
                        param_name,
                        f"Parameter '{param_name}' invalid timestamp format, "
                        "expected ISO format (YYYY-MM-DDTHH:MM:SS)",
                        cause=e,
                    ) from e
            elif not isinstance(value, datetime):
                raise _parameter_error(
                    "P102",
                    param_name,
                    f"Parameter '{param_name}' expected timestamp or ISO timestamp string",
                )

        elif param_type == "list[string]":
            if not isinstance(value, list):
                raise _parameter_error(
                    "P102",
                    param_name,
                    f"Parameter '{param_name}' expected list, got {type(value).__name__}",
                )
            if not all(isinstance(item, str) for item in value):
                raise _parameter_error(
                    "P102",
                    param_name,
                    f"Parameter '{param_name}' expected list of strings",
                )


def _parameter_error(
    code: str, name: str, message: str, cause: Exception | None = None
) -> ConfigSchemaError:
    """A parameter value that has the wrong type or format, pointed at its declaration."""
    return ConfigSchemaError(
        message,
        cause=cause,
        config_key=name,
        diagnostics=[
            diagnostic(code, message, pointer=json_pointer(("params", name)), origin=__name__)
        ],
    )


def _reference_error(
    code: str,
    message: str,
    *,
    pointer: str | None = None,
    file_path: str | None = None,
    expected: list[str] | None = None,
    hint: str | None = None,
) -> ReferenceResolutionError:
    """A reference failure, located at the entry that holds it.

    ``file_path`` stays on the error as the file the reference points at; the
    diagnostic names no file, so the config being loaded is attached to it.
    """
    return ReferenceResolutionError(
        message,
        file_path=file_path,
        diagnostics=[
            diagnostic(
                code,
                message,
                pointer=pointer,
                expected=expected,
                hint=hint,
                origin=__name__,
            )
        ],
    )


def _entry_error(
    code: str, message: str, pointer: str | None = None, hint: str | None = None
) -> ConfigError:
    """A malformed weave or thread entry, located at the entry."""
    return ConfigError(
        message,
        diagnostics=[diagnostic(code, message, pointer=pointer, hint=hint, origin=__name__)],
    )


def _circular_reference(
    ref: str, visited: dict[str, None], pointer: str
) -> ReferenceResolutionError:
    """The error for a ref that leads back to a file already being loaded."""
    chain = list(visited)
    cycle = [*chain[chain.index(ref) :], ref] if ref in chain else [*chain, ref]
    return _reference_error(
        "C302",
        f"Circular reference detected: {' -> '.join([*chain, ref])}",
        pointer=pointer,
        file_path=ref,
        expected=cycle,
    )


def _check_entry_refs(entries: list[Any], at: tuple[str | int, ...] = ()) -> None:
    """Reject a child entry whose ``ref`` isn't a string, before any ref is used as a key.

    Raises:
        ReferenceResolutionError: Naming no file of its own; the enclosing
            config's file is attached by the caller.
    """
    for index, entry in enumerate(entries):
        if isinstance(entry, dict) and "ref" in entry and not isinstance(entry["ref"], str):
            ref = entry["ref"]
            raise _reference_error(
                "C304",
                f"Reference must be a path string, got {type(ref).__name__}: {ref!r}",
                pointer=json_pointer((*at, index, "ref")),
            )


_SCHEME = re.compile(r"^([A-Za-z][A-Za-z0-9+.\-]*)://")


def _reference_scheme(ref: str) -> str | None:
    """The ``scheme`` of a ``scheme://path`` reference, or ``None`` when it has none."""
    match = _SCHEME.match(ref)
    return match.group(1) if match else None


def _outside_root(ref: str, pointer: str | None) -> ReferenceResolutionError:
    """The error for a reference that resolves outside the project root."""
    return _reference_error(
        "C303",
        f"Reference '{ref}' resolves outside project root",
        pointer=pointer,
        file_path=ref,
    )


def _unsupported_scheme(ref: str, scheme: str, pointer: str | None) -> ReferenceResolutionError:
    """The error for a reference that names a scheme; no scheme is supported."""
    return _reference_error(
        "C312",
        f"Unsupported reference scheme '{scheme}' in '{ref}'",
        pointer=pointer,
        file_path=ref,
        hint="Write the reference as a path relative to the project root.",
    )


def resolve_ref_path(
    ref: str, project_root: ConfigLocation | Path, pointer: str | None = None
) -> ConfigLocation:
    """Resolve a path-based reference to a :class:`ConfigLocation`.

    Args:
        ref: Path-based reference with typed extension, relative to project
            root (e.g., ``'dimensions/dim_customer.thread'``).
        project_root: The ``.weevr`` project directory. A bare
            :class:`pathlib.Path` is wrapped in a
            :class:`LocalConfigLocation` for backward compatibility.
        pointer: Where the ``ref`` sits in the referencing file, for the
            diagnostic of any failure.

    Returns:
        Resolved location for the referenced config file.

    Raises:
        ReferenceResolutionError: If the reference isn't a string (C304),
            names a ``scheme://`` (C312), is an absolute path (C311), or the
            referenced file does not exist or resolves outside the project
            root (C303).
        ConfigError: If the reference does not have a typed extension.
    """
    from weevr.config.locations import ConfigLocationError, LocalConfigLocation

    # Child entries reach here as unvalidated dicts, so the YAML may hold
    # any type. No file is named: the enclosing config's file is attached.
    if not isinstance(ref, str):
        raise _reference_error(
            "C304",
            f"Reference must be a path string, got {type(ref).__name__}: {ref!r}",
            pointer=pointer,
        )

    # A scheme is refused before any path handling, whatever it names.
    scheme = _reference_scheme(ref)
    if scheme is not None:
        raise _unsupported_scheme(ref, scheme, pointer)

    if isinstance(project_root, Path):
        project_root = LocalConfigLocation(project_root)

    suffix = Path(ref).suffix.lower()
    if suffix not in TYPED_EXTENSIONS:
        message = f"Unsupported extension '{suffix}'. Expected .thread, .weave, or .loom"
        raise ConfigError(
            message,
            file_path=ref,
            diagnostics=[
                diagnostic(
                    "C007",
                    message,
                    pointer=pointer,
                    expected=sorted(TYPED_EXTENSIONS),
                    origin=__name__,
                )
            ],
        )

    try:
        resolved = project_root.join(ref)
    except ConfigLocationError as exc:
        if any(d.code == "C311" for d in exc.diagnostics):
            raise _reference_error(
                "C311",
                f"Reference '{ref}' is an absolute path",
                pointer=pointer,
                file_path=ref,
                hint="Write the reference relative to the project root.",
            ) from exc
        raise _outside_root(ref, pointer) from exc
    except ValueError as exc:
        raise _outside_root(ref, pointer) from exc

    if not resolved.is_relative_to(project_root):
        raise _reference_error(
            "C303",
            f"Reference '{ref}' resolves outside project root",
            pointer=pointer,
            file_path=str(resolved),
        )
    if not resolved.exists():
        raise _reference_error(
            "C301",
            f"Referenced file not found: {ref}",
            pointer=pointer,
            file_path=str(resolved),
        )
    return resolved


def resolve_references(
    config: dict[str, Any],
    config_type: str,
    project_root: ConfigLocation | Path,
    runtime_params: dict[str, Any] | None = None,
    visited: dict[str, None] | None = None,
    authored: dict[str, Any] | None = None,
    at: tuple[str | int, ...] = (),
    *,
    scope: ParamScope | None = None,
    file: str | None = None,
    inline_scopes: ScopePositions | None = None,
    warnings: list[Diagnostic] | None = None,
    reads: BindingReads | None = None,
    pins_resolved: bool = False,
) -> list[ResolvedChild]:
    """Resolve references to other config files.

    Handles both external references (``ref`` key) and inline definitions
    (``name`` + body keys). Recursively loads referenced configs with
    circular reference detection. ``config`` itself is not modified.

    A ``ref:`` entry's ``params:`` pin values for the child's whole subtree:
    each resolves in ``config``'s scope, then binds in the child over any
    inherited value. A pin nothing beneath the entry reads warns ``P123``.

    Args:
        config: Config dict to resolve references in.
        config_type: Type of this config (``'weave'`` or ``'loom'``).
        project_root: The ``.weevr`` project directory. A bare
            :class:`pathlib.Path` is wrapped in a
            :class:`LocalConfigLocation` for backward compatibility.
        runtime_params: Runtime parameters to pass to child configs.
        visited: Refs already being loaded, in load order (for cycle detection).
        authored: ``config`` before variable substitution, so inline
            children can keep their authored form.
        at: Where ``config`` sits in its file; empty unless it is an
            inline weave in a loom.
        scope: ``config``'s param scope; each child opens its own from it.
            Defaults to the runtime params' root scope.
        file: ``config``'s file, named by errors at inline declarations.
        inline_scopes: The scopes :func:`resolve_config_variables` opened for
            ``config``'s inline children, by position; an inline weave
            reuses its scope from here.
        warnings: Receives the load warnings found (``P123``).
        reads: The bindings read so far, shared down the walk. Defaults to a
            fresh record.
        pins_resolved: Whether ``config`` came from the variable pass
            (:func:`resolve_config_variables`), which already resolved its
            entries' pins; otherwise they are resolved here.

    Returns:
        The resolved children in entry order: a loom's weaves (each with its
        threads) or a weave's threads. Empty for other config types.

    Raises:
        ReferenceResolutionError: If referenced file not found or circular
            reference detected.
        ConfigError: If an inline definition is missing a ``name`` field.
    """
    from weevr.config.attribution import attribute_errors
    from weevr.config.document_load import load_document
    from weevr.config.locations import LocalConfigLocation
    from weevr.config.validation import validate_inline_thread_types, validate_schema

    if isinstance(project_root, Path):
        project_root = LocalConfigLocation(project_root)

    if visited is None:
        visited = {}
    env = scope if scope is not None else ParamScope.root(runtime_params)
    if warnings is None:
        warnings = []
    if reads is None:
        reads = BindingReads()
    # Entry pins belong to this config: they resolve in its scope, once.
    parent_context = (
        None if pins_resolved else build_param_context(runtime_params, entry_params=env.values())
    )

    def authored_entry(key: str, index: int) -> dict[str, Any] | None:
        entries = (authored or {}).get(key)
        if isinstance(entries, list) and index < len(entries):
            entry = entries[index]
            return entry if isinstance(entry, dict) else None
        return None

    children: list[ResolvedChild] = []

    # Resolve weaves in loom
    if config_type == "loom" and "weaves" in config:
        _check_entry_refs(config["weaves"], (*at, "weaves"))
        for index, entry in enumerate(config["weaves"]):
            with skip_on_error():
                # Normalize: string entries become {"name": string}
                if isinstance(entry, str):
                    entry = {"name": entry}

                if isinstance(entry, dict) and "ref" in entry:
                    # External reference
                    ref = entry["ref"]
                    ref_pointer = json_pointer((*at, "weaves", index, "ref"))
                    if ref in visited:
                        raise _circular_reference(ref, visited, ref_pointer)

                    weave_path = resolve_ref_path(ref, project_root, ref_pointer)
                    visited[ref] = None
                    try:
                        with (
                            enter_child(
                                entry,
                                parent_context,
                                env,
                                reads,
                                (*at, "weaves", index),
                                warnings,
                                file=file,
                                alias_key="name",
                                defaults=config.get("defaults"),
                            ) as entered,
                            attribute_errors(weave_path),
                        ):
                            child_type, validated = load_document(weave_path)
                            child_dict = validated.model_dump(exclude_unset=True)

                            # Inject name from filename stem
                            if not child_dict.get("name"):
                                child_dict["name"] = weave_path.stem

                            # Set qualified key
                            child_dict["qualified_key"] = ref

                            weave_scope = entered.open(child_dict, str(weave_path))
                            resolved_child = resolve_scoped(
                                child_dict,
                                child_type,
                                weave_scope,
                                child_dict.get("defaults"),
                                runtime_params=runtime_params,
                                fabric_context=None,
                                file=str(weave_path),
                                reads=reads,
                            )

                            grandchildren = resolve_references(
                                resolved_child,
                                child_type,
                                project_root,
                                runtime_params,
                                visited.copy(),
                                authored=child_dict,
                                scope=weave_scope,
                                file=str(weave_path),
                                warnings=warnings,
                                reads=reads,
                                pins_resolved=True,
                            )
                        children.append(
                            ResolvedChild(
                                entry=entry,
                                data=resolved_child,
                                source_path=str(weave_path),
                                authored=child_dict,
                                children=grandchildren,
                            )
                        )
                    finally:
                        visited.pop(ref, None)
                elif not isinstance(entry, dict):
                    # Schema validation rejects other entry types before resolution.
                    continue
                elif not entry.get("name"):
                    raise _entry_error(
                        "C305",
                        "Inline weave definition requires a 'name' field",
                        json_pointer((*at, "weaves", index)),
                    )
                elif not is_inline_weave(entry):
                    # A bare name is not a reference: no file is loaded by name.
                    raise _name_only_entry(
                        "weave", entry["name"], json_pointer((*at, "weaves", index))
                    )
                else:
                    # Inline weave body.
                    weave_name = entry["name"]
                    # resolve_config_variables already resolved its variables
                    # with the weave's own defaults; validate and resolve its
                    # threads the way a weave file is.
                    validated = validate_schema(entry, "weave", at=(*at, "weaves", index))
                    inline = validated.model_dump(exclude_unset=True)
                    inline["qualified_key"] = weave_name
                    if "condition" in entry:
                        inline["condition"] = entry["condition"]
                    inline_authored = authored_entry("weaves", index)
                    inline_at = (*at, "weaves", index)
                    # The variable pass opened this weave's scope; a caller
                    # that skipped it gets the scope opened here instead.
                    weave_scope = (inline_scopes or {}).get(inline_at)
                    if weave_scope is None:
                        weave_scope = env.open_child(inline.get("params"), file=file, at=inline_at)
                    children.append(
                        ResolvedChild(
                            entry=entry,
                            data=inline,
                            authored=inline_authored,
                            children=resolve_references(
                                inline,
                                "weave",
                                project_root,
                                runtime_params,
                                visited.copy(),
                                authored=inline_authored,
                                at=inline_at,
                                scope=weave_scope,
                                file=file,
                                warnings=warnings,
                                reads=reads,
                                pins_resolved=pins_resolved,
                            ),
                            at=inline_at,
                        )
                    )

    # Resolve threads in weave
    elif config_type == "weave" and "threads" in config:
        # Validate: duplicate refs without aliases
        _check_entry_refs(config["threads"], (*at, "threads"))
        ref_entries = [e for e in config["threads"] if isinstance(e, dict) and e.get("ref")]
        ref_counts = Counter(e["ref"] for e in ref_entries)
        for ref_val, count in ref_counts.items():
            if count > 1:
                aliases = [e.get("as") for e in ref_entries if e["ref"] == ref_val]
                if not all(aliases):
                    first = next(
                        i
                        for i, e in enumerate(config["threads"])
                        if isinstance(e, dict) and e.get("ref") == ref_val and not e.get("as")
                    )
                    raise _entry_error(
                        "C307",
                        f"Thread ref '{ref_val}' appears multiple times without "
                        f"'as' aliases. Use 'as' to give each instance a unique name.",
                        json_pointer((*at, "threads", first)),
                        hint="Add an 'as' alias to each instance.",
                    )

        seen_effective_names: set[str] = set()

        for index, entry in enumerate(config["threads"]):
            with skip_on_error():
                # Normalize: string entries become {"name": string}
                if isinstance(entry, str):
                    entry = {"name": entry}

                if isinstance(entry, dict) and "ref" in entry:
                    # External reference
                    ref = entry["ref"]
                    ref_pointer = json_pointer((*at, "threads", index, "ref"))
                    if ref in visited:
                        raise _circular_reference(ref, visited, ref_pointer)

                    thread_path = resolve_ref_path(ref, project_root, ref_pointer)
                    visited[ref] = None
                    try:
                        # Entry params belong to this weave, so they resolve
                        # outside the referenced thread's attribution scope.
                        with (
                            enter_child(
                                entry,
                                parent_context,
                                env,
                                reads,
                                (*at, "threads", index),
                                warnings,
                                file=file,
                                alias_key="as",
                                defaults=config.get("defaults"),
                            ) as entered,
                            attribute_errors(thread_path),
                        ):
                            child_type, validated = load_document(thread_path)
                            child_dict = validated.model_dump(exclude_unset=True)

                            # Inject name from filename stem
                            if not child_dict.get("name"):
                                child_dict["name"] = thread_path.stem

                            # Set qualified key
                            child_dict["qualified_key"] = ref

                            thread_scope = entered.open(child_dict, str(thread_path))
                            context = build_param_context(
                                runtime_params, entry_params=thread_scope.values()
                            )
                            resolved_child = substitute(
                                child_dict,
                                context,
                                thread_scope,
                                reads,
                                defaults=child_dict.get("defaults"),
                            )

                        # Track effective name uniqueness
                        effective_name = (
                            entry.get("as") or resolved_child.get("name") or thread_path.stem
                        )
                        if effective_name in seen_effective_names:
                            raise _entry_error(
                                "C306",
                                f"Duplicate effective thread name '{effective_name}' "
                                f"in weave. Use 'as' to give each instance a unique name.",
                                json_pointer((*at, "threads", index)),
                                hint="Use 'as' to give each instance a unique name.",
                            )
                        seen_effective_names.add(effective_name)

                        children.append(
                            ResolvedChild(
                                entry=entry,
                                data=resolved_child,
                                source_path=str(thread_path),
                                authored=child_dict,
                            )
                        )
                    finally:
                        visited.pop(ref, None)
                elif not isinstance(entry, dict):
                    # Schema validation rejects other entry types before resolution.
                    continue
                elif not entry.get("name"):
                    raise _entry_error(
                        "C305",
                        "Inline thread definition requires a 'name' field",
                        json_pointer((*at, "threads", index)),
                    )
                elif not is_inline_thread(entry):
                    # A bare name is not a reference: no file is loaded by name.
                    raise _name_only_entry(
                        "thread", entry["name"], json_pointer((*at, "threads", index))
                    )
                else:
                    # Inline thread body.
                    # The cascade and load checks read the body before
                    # hydration, so its value types are checked here.
                    validate_inline_thread_types(entry, at=(*at, "threads", index))
                    inline = entry.copy()
                    inline["qualified_key"] = entry.get("name", "")

                    # Track effective name uniqueness
                    effective_name = entry.get("as") or entry.get("name", "")
                    if effective_name in seen_effective_names:
                        raise _entry_error(
                            "C306",
                            f"Duplicate effective thread name '{effective_name}' "
                            f"in weave. Use 'as' to give each instance a unique name.",
                            json_pointer((*at, "threads", index)),
                            hint="Use 'as' to give each instance a unique name.",
                        )
                    seen_effective_names.add(effective_name)

                    children.append(
                        ResolvedChild(
                            entry=entry,
                            data=inline,
                            authored=authored_entry("threads", index),
                            at=(*at, "threads", index),
                        )
                    )

    return children
