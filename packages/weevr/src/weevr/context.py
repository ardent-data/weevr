"""Context class — the user-facing entry point for weevr."""

from __future__ import annotations

import logging
import os
import time
from dataclasses import replace
from pathlib import Path
from types import MappingProxyType
from typing import Any

from pyspark.sql import SparkSession

from weevr.config.collect import diagnostics_of
from weevr.config.document import ScopeRegistries, registry_as_dict
from weevr.config.loader import collect_resolved_config, load_resolved_config
from weevr.config.locations import ConfigLocation, LocalConfigLocation, RemoteConfigLocation
from weevr.config.manifest import param_manifest
from weevr.config.manifest_types import ParamManifest
from weevr.config.onelake import resolve_connection_path
from weevr.config.parser import TYPED_EXTENSIONS, declared_kind
from weevr.config.project import resolve_project_connections
from weevr.config.root import PROJECT_FILE, project_root_not_found, read_project
from weevr.config.run_store import RunStoreLocation, resolve_run_store
from weevr.config.validation import (
    in_thread,
    partition_diagnostics,
    rename_column_set_diagnostics,
    render_diagnostics,
)
from weevr.delta import probe_delta_table_exists
from weevr.diagnostics import Diagnostic, diagnostic
from weevr.diagnostics.positions import json_pointer
from weevr.engine.executor import execute_thread, resolve_with_block
from weevr.engine.lookups import resolve_lookup_frames
from weevr.engine.planner import (
    build_plan,
    build_producer_map,
    find_in_run_producers,
    resolve_lookup_names,
)
from weevr.engine.runner import execute_loom, execute_weave
from weevr.errors import (
    ConfigError,
    ConfigLocationError,
    DataValidationError,
    ExecutionError,
)
from weevr.model.connection import OneLakeConnection
from weevr.model.execution import LogLevel, resolve_effective_execution
from weevr.model.lookup import Lookup
from weevr.model.loom import Loom
from weevr.model.project import Project, RunStore
from weevr.model.source import Source
from weevr.model.thread import Thread
from weevr.model.weave import ConditionSpec, Weave
from weevr.result import ExecutionMode, LoadedConfig, PreviewThreadMetadata, RunResult
from weevr.telemetry.logging import configure_logging

logger = logging.getLogger(__name__)

_DEFAULT_LAKEHOUSE_FILES = "/lakehouse/default/Files/"
"""Where Tier 1 looks for a project named without a path."""


class Context:
    """Entry point for all weevr operations.

    Wraps a SparkSession with a project reference, resolved parameters,
    and execution configuration. Provides ``run()`` for execution and
    ``load()`` for model inspection.

    Args:
        spark: Active SparkSession (required).
        project: Project identifier. Accepts three forms:

            - **Direct path** (e.g., ``"/path/to/sales"``): an absolute path,
              or a relative path to an existing directory, used as-is.
            - **Qualified** (with *workspace* and *lakehouse*): resolved via
              OneLake ABFS path to ``Files/<name>.weevr/``.
            - **Simple name** (e.g., ``"my_project"``): resolved via the
              default lakehouse at ``/lakehouse/default/Files/<name>.weevr/``.

            Whichever form resolves it, the project directory must hold a
            ``weevr.yaml`` (``kind: project``); the ``.weevr`` folder suffix
            is only the naming convention of the name forms. Without one,
            construction fails with ``C209``, listing the location probed.
        params: Runtime parameter overrides. They are the run root of every
            config this Context loads, and each load judges them on its own:
            a param the loaded config neither declares nor references warns
            ``P121``. Shared environment params therefore warn when a config
            doesn't use them.
        log_level: Logging verbosity — ``"minimal"``, ``"standard"`` (default),
            ``"verbose"``, or ``"debug"``.
        workspace: Fabric workspace ID for cross-lakehouse resolution.
        lakehouse: Fabric lakehouse ID for cross-lakehouse resolution.
        run_store: Run Store settings that replace the ``run_store`` block of
            ``weevr.yaml`` whole (see :attr:`run_store`).
        strict_params: Overrides every document's ``strict_params:`` for each
            config this Context loads. ``True`` fails the load on a
            ``${param.x}`` no declaration in its chain covers (``P120``) and
            on a param in ``params`` nothing declares or references
            (``P121``); ``False`` keeps both warnings even where a document
            sets ``strict_params: true``. ``None`` (the default) leaves each
            document's setting, most specific wins, absent is lenient. Under
            strict params, pass only the params each run needs, or declare
            the shared ones: a declared param counts as used.
    """

    def __init__(
        self,
        spark: SparkSession,
        project: str | Path,
        *,
        params: dict[str, Any] | None = None,
        log_level: str | None = None,
        workspace: str | None = None,
        lakehouse: str | None = None,
        run_store: RunStore | None = None,
        strict_params: bool | None = None,
    ) -> None:
        """Initialize a Context with a SparkSession and project reference."""
        if not isinstance(spark, SparkSession):
            raise TypeError(f"'spark' must be a SparkSession, got {type(spark).__name__}")
        if run_store is not None and not isinstance(run_store, RunStore):
            raise TypeError(f"'run_store' must be a RunStore, got {type(run_store).__name__}")

        # An explicit argument is a runtime override that beats YAML
        # execution blocks. When omitted, STANDARD applies provisionally
        # and the effective YAML level (if any) takes over at run()/load().
        self._explicit_log_level = log_level is not None
        if log_level is None:
            resolved_level = LogLevel.STANDARD
        else:
            try:
                resolved_level = LogLevel(log_level)
            except ValueError:
                valid = ", ".join(f"'{v.value}'" for v in LogLevel)
                raise ValueError(
                    f"Invalid log_level '{log_level}'. Must be one of: {valid}"
                ) from None

        self._spark = spark
        self._params = params
        self._strict_params = strict_params
        self._log_level = resolved_level
        self._project_root, self._project = self._resolve_project_path(
            project, workspace, lakehouse
        )
        self._run_store_override = run_store
        self._connections: dict[str, OneLakeConnection] | None = None
        configure_logging(self._log_level)

    def _resolve_project_path(
        self,
        project: str | Path,
        workspace: str | None,
        lakehouse: str | None,
    ) -> tuple[ConfigLocation, Project]:
        """Resolve the project directory and load its ``weevr.yaml``.

        The directory comes from three tiers: a direct path (absolute, or an
        existing relative directory), then the OneLake path when
        ``workspace`` and ``lakehouse`` are given, then the default
        lakehouse. Whichever tier resolves it, the directory must hold a
        ``weevr.yaml``.

        Args:
            project: Project name or direct path.
            workspace: Optional Fabric workspace ID.
            lakehouse: Optional Fabric lakehouse ID.

        Returns:
            The project root and its project document. Tier 1 and Tier 3
            give a :class:`LocalConfigLocation`; Tier 2 a
            :class:`RemoteConfigLocation` bound to ``self._spark``.

        Raises:
            ConfigError: No ``weevr.yaml`` at the resolved directory (C209,
                listing what was probed), or it fails to load.
        """
        root, missing_hint = self._project_directory(project, workspace, lakehouse)
        loaded = read_project(root)
        if loaded is None:
            raise project_root_not_found([root.join(PROJECT_FILE)], hint=missing_hint)
        return root, loaded

    def _project_directory(
        self,
        project: str | Path,
        workspace: str | None,
        lakehouse: str | None,
    ) -> tuple[ConfigLocation, str | None]:
        """The project directory named by ``project``, before ``weevr.yaml`` is probed.

        Returns:
            The directory, and the hint for a missing ``weevr.yaml`` there —
            set only when the default-lakehouse tier produced the directory.
        """
        project_str = str(project)

        # Tier 3: Direct path — absolute, or an existing relative directory when no
        # OneLake location is named (a local folder must not shadow tier 2)
        direct = os.path.isabs(project_str) or (
            workspace is None and lakehouse is None and os.path.isdir(project_str)
        )
        if direct:
            return LocalConfigLocation(Path(project_str)), None

        # Normalize: strip .weevr suffix if already present so Tier 1/2
        # don't produce a double extension (e.g. "my_project.weevr.weevr").
        base_name = project_str.removesuffix(".weevr")

        # Tier 2: OneLake qualified — workspace + lakehouse provided
        if workspace and lakehouse:
            uri = (
                f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com"
                f"/{lakehouse}/Files/{base_name}.weevr"
            )
            return RemoteConfigLocation(uri, self._spark), None

        # Tier 1: Default lakehouse
        return (
            LocalConfigLocation(Path(f"{_DEFAULT_LAKEHOUSE_FILES}{base_name}.weevr")),
            "Add a weevr.yaml to that default lakehouse folder, or pass workspace "
            "and lakehouse, or a direct project path.",
        )

    @property
    def spark(self) -> SparkSession:
        """The underlying SparkSession."""
        return self._spark

    @property
    def params(self) -> dict[str, Any] | None:
        """Runtime parameter overrides."""
        return self._params

    @property
    def log_level(self) -> LogLevel:
        """Configured logging verbosity."""
        return self._log_level

    @property
    def project_root(self) -> ConfigLocation:
        """Resolved project root as a :class:`ConfigLocation`.

        Returns a :class:`LocalConfigLocation` when the project is on the
        local filesystem (Tier 1 default lakehouse or Tier 3 direct path)
        and a :class:`RemoteConfigLocation` when the project is qualified
        by ``workspace`` and ``lakehouse`` (Tier 2 OneLake).
        """
        return self._project_root

    @property
    def project(self) -> Project:
        """The project document (``weevr.yaml``) at the project root."""
        return self._project

    @property
    def run_store(self) -> RunStoreLocation | None:
        """Where this project's run outputs live, or ``None`` when nowhere.

        Resolved on each access by
        :func:`weevr.config.run_store.resolve_run_store` from the project
        root and ``weevr.yaml``'s ``run_store`` block, which the
        ``run_store`` argument replaces whole. ``run_store.connection`` is
        looked up by name in the top-level connections of the config this
        Context loaded last, then in ``weevr.yaml``'s ``connections``,
        resolved with this Context's params (over the project's defaults)
        and Fabric context. A name neither declares raises
        :class:`~weevr.errors.ConfigLocationError` (``R201``). ``None`` means
        the root implies no store and no connection names one; persistence is
        then skipped.
        """
        return resolve_run_store(
            self._project,
            self._project_root,
            self._run_store_override,
            connections=self._run_store_connections(),
        )

    def _run_store_connections(self) -> dict[str, OneLakeConnection] | None:
        """The connections ``run_store.connection`` is looked up in, nearest first by name.

        The project's connection of that name is resolved only when the
        loaded config doesn't declare it; the project's others are not
        resolved at all.
        """
        loaded = dict(self._connections or {})
        settings = self._run_store_override or self._project.run_store
        name = settings.connection if settings is not None else None
        if name is None or name in loaded:
            return loaded or None
        from weevr.config.fabric import build_fabric_context

        project = resolve_project_connections(
            self._project,
            str(self._project_root.join(PROJECT_FILE)),
            runtime_params=self._params,
            fabric_context=build_fabric_context(self._spark),
            strict_params=self._strict_params,
            names={name},
        )
        return {**(project or {}), **loaded} or None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def load(self, path: str | Path) -> LoadedConfig:
        """Load and validate a config file, returning a model wrapper.

        Parses, resolves, and hydrates the config at *path* (relative to
        the project root) without executing anything. The returned
        :class:`LoadedConfig` exposes the underlying model and, for
        weave/loom configs, a lazily-built execution plan.

        Args:
            path: Path to a config file, relative to the project root.

        Returns:
            A :class:`LoadedConfig` wrapping the hydrated model.
        """
        loaded = self._load(path)
        self._apply_effective_log_level(loaded)
        return loaded

    def _apply_effective_log_level(self, loaded: LoadedConfig) -> None:
        """Apply the effective YAML log level.

        No-op when an explicit ``Context(log_level=...)`` argument is in
        force. The source is the top-level ``execution:`` block on a Loom or
        Weave config. Thread-scoped execution settings are not applied
        (uniformly unapplied in v1.x).
        """
        if self._explicit_log_level:
            return
        block = None
        if loaded.config_type in ("loom", "weave"):
            block = getattr(loaded.model, "execution", None)
        effective = resolve_effective_execution(None, block).log_level
        if effective != self._log_level:
            self._log_level = effective
            configure_logging(effective)

    def run(
        self,
        path: str | Path,
        *,
        mode: str = "execute",
        tags: list[str] | None = None,
        threads: list[str] | None = None,
        sample_rows: int = 100,
    ) -> RunResult:
        """Run a config file in the specified mode.

        Args:
            path: Path to a config file, relative to the project root.
            mode: Execution mode — ``"execute"`` (default), ``"validate"``,
                ``"plan"``, or ``"preview"``.
            tags: Run only threads matching any of these tags.
            threads: Run only threads with these names.
            sample_rows: Maximum rows for preview mode (default 100).

        Returns:
            A :class:`RunResult` describing the outcome. In ``"validate"``
            mode, every config problem — load, plan and source existence — is
            in ``RunResult.diagnostics`` and nothing is raised for them, and
            ``RunResult.param_manifest`` lists the params the config takes,
            even when the load fails.

        Raises:
            ValueError: If *mode* is invalid or both *tags* and *threads*
                are provided.
            ConfigError: In ``"execute"``, ``"plan"`` and ``"preview"`` modes,
                when the config has problems; ``.diagnostics`` lists them all.
        """
        try:
            resolved_mode = ExecutionMode(mode)
        except ValueError:
            valid = ", ".join(f"'{m.value}'" for m in ExecutionMode)
            raise ValueError(f"Invalid mode '{mode}'. Must be one of: {valid}") from None

        if tags is not None and threads is not None:
            raise ValueError("'tags' and 'threads' are mutually exclusive")

        start_ns = time.monotonic_ns()
        if resolved_mode is ExecutionMode.VALIDATE:
            result = self._with_project(self._validate(path))
            result.param_manifest = self._param_manifest(path)
            result.duration_ms = (time.monotonic_ns() - start_ns) // 1_000_000
            return result

        try:
            loaded = self._load(path)
            self._check_standalone_column_sets(loaded, path)
        except ConfigError as exc:
            # Report the plan problems of the files that did load with the
            # load problems, so one failed run lists everything to fix.
            exc.diagnostics = [*exc.diagnostics, *self._plan_problems(path)]
            raise

        # Single application point for the effective YAML log level —
        # before mode dispatch, so all four modes behave identically.
        self._apply_effective_log_level(loaded)

        try:
            if resolved_mode is ExecutionMode.EXECUTE:
                result = self._run_execute(loaded, tags=tags, thread_names=threads)
            elif resolved_mode is ExecutionMode.PLAN:
                result = self._run_plan(loaded, tags=tags, thread_names=threads)
            else:
                result = self._run_preview(
                    loaded, tags=tags, thread_names=threads, sample_rows=sample_rows
                )
        except (ExecutionError, DataValidationError) as exc:
            duration_ms = (time.monotonic_ns() - start_ns) // 1_000_000
            return self._with_project(
                RunResult(
                    status="failure",
                    mode=resolved_mode,
                    config_type=loaded.config_type,
                    config_name=loaded.config_name,
                    duration_ms=duration_ms,
                    warnings=[str(exc)],
                    diagnostics=loaded.diagnostics,
                )
            )

        result.duration_ms = (time.monotonic_ns() - start_ns) // 1_000_000
        result.diagnostics = [*loaded.diagnostics, *result.diagnostics]
        return self._with_project(result)

    def _with_project(self, result: RunResult) -> RunResult:
        """Record the project root and identity the run used on its result."""
        result.project = self._project
        result.project_root = str(self._project_root)
        return result

    def _check_standalone_column_sets(self, loaded: LoadedConfig, path: str | Path) -> None:
        """Fail a thread run on its own whose rename steps name sets it doesn't define.

        Loading a thread file only warns about such a set, since the thread
        may be valid inside the weave or loom that defines it. Run on its
        own, though, the thread sees only its own column sets, and the
        rename would silently do nothing.

        Raises:
            ConfigError: If a rename step names a set the thread doesn't define.
        """
        model = loaded.model
        if loaded.config_type != "thread" or not isinstance(model, Thread):
            return
        errors, _ = partition_diagnostics(
            rename_column_set_diagnostics(
                model.model_dump(), available_column_sets=set(model.column_sets or {})
            )
        )
        if errors:
            file = str(self._resolve_config_path(path))
            located = in_thread(errors, loaded.config_name, file=file)
            raise ConfigError(
                "; ".join(d.message for d in located),
                file_path=file,
                diagnostics=located,
            )

    # ------------------------------------------------------------------
    # Execute mode
    # ------------------------------------------------------------------

    def _run_execute(
        self,
        loaded: LoadedConfig,
        *,
        tags: list[str] | None = None,
        thread_names: list[str] | None = None,
    ) -> RunResult:
        """Execute a resolved config and wrap the result."""
        if loaded.config_type == "thread":
            model = loaded.model
            if not isinstance(model, Thread):
                raise TypeError(f"Expected Thread, got {type(model).__name__}")
            from weevr.telemetry.collector import SpanCollector
            from weevr.telemetry.span import generate_trace_id

            thread_collector = SpanCollector(generate_trace_id())
            engine_result = execute_thread(
                self._spark,
                model,
                collector=thread_collector,
                resolved_params=self._params,
                connections=model.connections,
                scope=loaded.document.effective,
            )
            if engine_result.status not in ("success", "failure"):
                raise ExecutionError(
                    f"Unexpected thread execution status: '{engine_result.status}'"
                )
            result = RunResult(
                status=engine_result.status,  # type: ignore[arg-type]
                mode=ExecutionMode.EXECUTE,
                config_type="thread",
                config_name=loaded.config_name,
                detail=engine_result,
                telemetry=engine_result.telemetry,
                warnings=self._collect_thread_warnings(engine_result),
            )
            result.thread_models = MappingProxyType({loaded.config_name: model})
            return result

        if loaded.config_type == "weave":
            model = loaded.model
            if not isinstance(model, Weave):
                raise TypeError(f"Expected Weave, got {type(model).__name__}")
            weave_threads = loaded.document.weave_threads().get(loaded.config_name, {})
            weave_threads, warnings = self._filter_threads(
                weave_threads, tags=tags, thread_names=thread_names
            )

            if not weave_threads:
                return RunResult(
                    status="success",
                    mode=ExecutionMode.EXECUTE,
                    config_type="weave",
                    config_name=loaded.config_name,
                    warnings=warnings,
                )

            filtered_entries = [e for e in model.threads if e.name in weave_threads]
            weave_lookups = dict(model.lookups) if model.lookups else None
            plan = build_plan(
                weave_name=loaded.config_name,
                threads=weave_threads,
                thread_entries=filtered_entries,
                lookups=weave_lookups,
            )
            # Build thread condition map from ThreadEntry conditions,
            # keyed by EFFECTIVE name — an aliased entry's condition
            # keyed by its raw name would silently never evaluate
            thread_conditions: dict[str, ConditionSpec] = {}
            for te in model.threads:
                if te.condition is not None:
                    thread_conditions[te.effective_name] = te.condition

            from weevr.telemetry.collector import SpanCollector
            from weevr.telemetry.span import generate_trace_id

            # The weave's effective trace gates collector creation for
            # standalone weave runs. (Standalone thread runs always trace —
            # thread-scoped execution settings are unapplied in v1.x.)
            standalone_execution = resolve_effective_execution(None, model.execution)
            weave_collector = (
                SpanCollector(generate_trace_id()) if standalone_execution.trace else None
            )

            scope = loaded.document.effective
            engine_result = execute_weave(
                self._spark,
                plan,
                weave_threads,
                collector=weave_collector,
                thread_conditions=thread_conditions if thread_conditions else None,
                params=self._params,
                pre_steps=list(model.pre_steps) if model.pre_steps else None,
                post_steps=list(model.post_steps) if model.post_steps else None,
                lookups=dict(model.lookups) if model.lookups else None,
                variables=registry_as_dict(scope.variables),
                column_set_defs=registry_as_dict(scope.column_sets),
                connections=dict(model.connections) if model.connections else None,
                execution=standalone_execution,
                weave_name=loaded.config_name,
                effective_lookups=scope.lookups,
                thread_scopes=_thread_scopes(loaded),
            )
            if engine_result.status not in ("success", "failure", "partial"):
                raise ExecutionError(f"Unexpected weave execution status: '{engine_result.status}'")
            result = RunResult(
                status=engine_result.status,  # type: ignore[arg-type]
                mode=ExecutionMode.EXECUTE,
                config_type="weave",
                config_name=loaded.config_name,
                detail=engine_result,
                telemetry=engine_result.telemetry,
                warnings=warnings + self._collect_thread_warnings(engine_result),
            )
            result.thread_models = MappingProxyType(dict(weave_threads))
            return result

        # loom
        model = loaded.model
        if not isinstance(model, Loom):
            raise TypeError(f"Expected Loom, got {type(model).__name__}")
        all_warnings: list[str] = []

        filtered_weaves: dict[str, Weave] = {}
        filtered_threads: dict[str, dict[str, Thread]] = {}
        weave_models = loaded.document.weaves()
        weave_threads = loaded.document.weave_threads()
        for weave_entry in model.weaves:
            weave_name = weave_entry.effective_name
            weave = weave_models.get(weave_name)
            if weave is None:
                continue
            wt = weave_threads.get(weave_name, {})
            wt, warnings = self._filter_threads(wt, tags=tags, thread_names=thread_names)
            all_warnings.extend(warnings)
            if wt:
                filtered_weaves[weave_name] = weave
                filtered_threads[weave_name] = wt

        if not filtered_weaves:
            return RunResult(
                status="success",
                mode=ExecutionMode.EXECUTE,
                config_type="loom",
                config_name=loaded.config_name,
                warnings=all_warnings,
            )

        # Baseline for per-weave log-level overrides; None suppresses YAML
        # levels entirely (explicit Context argument in force).
        baseline_level = None if self._explicit_log_level else self._log_level
        engine_result = execute_loom(
            self._spark,
            model,
            filtered_weaves,
            filtered_threads,
            params=self._params,
            log_level=baseline_level,
            document=loaded.document,
        )
        result = RunResult(
            status=engine_result.status,
            mode=ExecutionMode.EXECUTE,
            config_type="loom",
            config_name=loaded.config_name,
            detail=engine_result,
            telemetry=engine_result.telemetry,
            warnings=all_warnings + self._collect_thread_warnings(engine_result),
        )
        merged_threads: dict[str, Any] = {}
        for thread_map in filtered_threads.values():
            merged_threads.update(thread_map)
        result.thread_models = MappingProxyType(merged_threads)
        return result

    # ------------------------------------------------------------------
    # Thread filtering
    # ------------------------------------------------------------------

    @staticmethod
    def _collect_thread_warnings(detail: Any) -> list[str]:
        """Gather per-thread warnings from an engine result detail tree.

        Threads surface non-fatal conditions (e.g. an unattributable
        write count) on ``ThreadResult.warnings``; they must reach
        ``RunResult.warnings`` at every scope.
        """
        threads: list[Any]
        if hasattr(detail, "weave_results"):
            threads = [tr for wr in detail.weave_results for tr in wr.thread_results]
        elif hasattr(detail, "thread_results"):
            threads = list(detail.thread_results)
        else:
            threads = [detail]
        collected: list[str] = []
        for tr in threads:
            if getattr(tr, "warnings", None):
                collected.extend(tr.warnings)
        return collected

    @staticmethod
    def _filter_threads(
        all_threads: dict[str, Thread],
        *,
        tags: list[str] | None = None,
        thread_names: list[str] | None = None,
    ) -> tuple[dict[str, Thread], list[str]]:
        """Filter threads by tags or explicit names.

        Returns:
            A tuple of (filtered threads dict, warning messages).
        """
        if tags is None and thread_names is None:
            return all_threads, []

        if thread_names is not None:
            requested = set(thread_names)
            filtered = {n: t for n, t in all_threads.items() if n in requested}
        else:
            # tags is guaranteed non-None here — the (None, None) case
            # returns early at line 432-433.
            requested_tags = set(tags)  # type: ignore[arg-type]
            filtered = {
                n: t
                for n, t in all_threads.items()
                if t.tags is not None and requested_tags & set(t.tags)
            }

        warnings: list[str] = []
        if not filtered:
            warnings.append("No threads matched filter")
        return filtered, warnings

    # ------------------------------------------------------------------
    # Validate mode
    # ------------------------------------------------------------------

    def _validate(self, path: str | Path) -> RunResult:
        """Validate mode: report every config, plan and source problem; raise for none.

        Files that fail to load are reported and left out; the plan and
        source checks run on the rest. Only failures that aren't config
        problems (a lost Spark session) raise.
        """
        loaded, errors = self._load_for_validate(path)
        if loaded is None:
            return RunResult(
                status="failure",
                mode=ExecutionMode.VALIDATE,
                config_type=_display_kind(self._resolve_config_path(path)),
                config_name=Path(str(path)).stem,
                validation_errors=[d.message for d in errors] or None,
                diagnostics=errors,
            )
        try:
            self._check_standalone_column_sets(loaded, path)
        except ConfigError as exc:
            errors.extend(diagnostics_of([exc]))
        self._apply_effective_log_level(loaded)
        try:
            return self._run_validate(loaded, errors)
        except (ExecutionError, DataValidationError) as exc:
            return RunResult(
                status="failure",
                mode=ExecutionMode.VALIDATE,
                config_type=loaded.config_type,
                config_name=loaded.config_name,
                warnings=[str(exc)],
                diagnostics=[*errors, *loaded.diagnostics],
            )

    def _param_manifest(self, path: str | Path) -> ParamManifest | None:
        """The params *path* takes, read apart from the load so a failed load still has it.

        Returns ``None`` when a document cannot be read; the load reports why.
        """
        try:
            return param_manifest(self._resolve_config_path(path), project_root=self._project_root)
        except ConfigError as exc:
            logger.debug("No param manifest for %s: %s", path, exc)
            return None

    def _run_validate(self, loaded: LoadedConfig, load_errors: list[Diagnostic]) -> RunResult:
        """Check the DAG and source existence of a loaded config, beside its load problems."""
        diagnostics: list[Diagnostic] = [*load_errors, *loaded.diagnostics]
        validation_errors: list[str] = [d.message for d in load_errors]

        for issue in loaded.validate_plans():
            if issue.diagnostic is not None:
                diagnostics.append(issue.diagnostic)
            if issue.error is None:
                validation_errors.append(issue.message)
            elif loaded.config_type == "weave":
                validation_errors.append(f"DAG validation failed: {issue.message}")
            else:
                validation_errors.append(
                    f"DAG validation failed for weave '{issue.weave_name}': {issue.message}"
                )

        all_threads = self._collect_all_threads(loaded)
        source_errors = self._source_existence_diagnostics(all_threads)
        diagnostics.extend(source_errors)
        validation_errors.extend(d.message for d in source_errors)

        failed = any(d.is_error for d in diagnostics)
        return RunResult(
            status="failure" if failed else "success",
            mode=ExecutionMode.VALIDATE,
            config_type=loaded.config_type,
            config_name=loaded.config_name,
            validation_errors=validation_errors if validation_errors else None,
            diagnostics=diagnostics,
            thread_models=_thread_models(loaded),
        )

    def _source_existence_diagnostics(self, threads: dict[str, Thread]) -> list[Diagnostic]:
        """Check that all sources referenced by threads exist, as coded diagnostics.

        Existence is a metadata question: Delta sources, including
        connection-form ones, go through the catalog or Delta-log check,
        file sources through a filesystem probe — no reads, no schema
        inference, no Spark jobs.

        Returns:
            One diagnostic per problem source: ``S101`` for a location that
            does not exist, ``S102`` for one whose probe failed, and the
            location error's own code (an undefined connection's ``R201``)
            for a connection-form source whose location cannot be resolved. Each
            names the thread (``node``) and the source alias
            (``resolved_path``); the message gives the location.
        """
        errors: list[Diagnostic] = []
        checked: set[str] = set()
        # Sources another thread in this run writes are created by the run
        # itself, in dependency order — they need not exist yet.
        producers = build_producer_map(threads)

        for thread_name, thread in threads.items():
            for source_name, source in thread.sources.items():
                if source.lookup is not None:
                    continue
                try:
                    location = self._source_location(source, thread.connections)
                except ConfigLocationError as exc:
                    cause = exc.diagnostics[0]
                    errors.append(
                        replace(
                            cause,
                            message=f"Source '{source_name}' in thread '{thread_name}': {exc}",
                            node=thread_name,
                            resolved_path=json_pointer(("sources", source_name)),
                        )
                    )
                    continue
                if location is None:
                    continue
                source_type, resolve_path = location
                if find_in_run_producers(resolve_path, producers, reader=thread_name):
                    continue
                if resolve_path in checked:
                    continue
                checked.add(resolve_path)

                try:
                    exists = self._source_location_exists(source_type, resolve_path)
                except Exception as exc:
                    logger.warning("Existence probe failed for '%s': %s", resolve_path, exc)
                    errors.append(
                        diagnostic(
                            "S102",
                            f"Source '{source_name}' in thread '{thread_name}' "
                            f"could not be verified: {resolve_path}",
                            node=thread_name,
                            resolved_path=json_pointer(("sources", source_name)),
                        )
                    )
                    continue
                if not exists:
                    errors.append(
                        diagnostic(
                            "S101",
                            f"Source '{source_name}' in thread '{thread_name}' "
                            f"not found: {resolve_path}",
                            node=thread_name,
                            resolved_path=json_pointer(("sources", source_name)),
                        )
                    )
        return errors

    def _awaited_in_run_table(
        self,
        thread_name: str,
        thread: Thread,
        lookups: dict[str, Lookup],
        producers: dict[str, list[str]],
    ) -> tuple[str, list[str]] | None:
        """Find a table the thread reads that another thread in the run writes but hasn't yet.

        Checks the thread's own sources and the sources of the lookups it
        uses (lookup-bound sources and ``resolve`` steps). A location that
        can't be resolved or probed is left to the normal read, which
        reports the real error.

        Returns:
            ``(location, writer threads)`` for the first such table, else ``None``.
        """
        bound = {alias for alias, src in thread.sources.items() if src.lookup is not None}
        names = {src.lookup for src in thread.sources.values() if src.lookup is not None}
        names |= resolve_lookup_names(thread) - bound
        candidates = [src for src in thread.sources.values() if src.lookup is None]
        candidates += [lookups[name].source for name in sorted(names) if name in lookups]

        for source in candidates:
            try:
                location = self._source_location(source, thread.connections)
            except ValueError:
                continue
            if location is None:
                continue
            source_type, path = location
            writers = find_in_run_producers(path, producers, reader=thread_name)
            if not writers:
                continue
            try:
                exists = self._source_location_exists(source_type, path)
            except Exception as exc:
                # Unknown: don't skip. The normal read reports the real error.
                logger.warning("Preview could not check in-run table '%s': %s", path, exc)
                continue
            if not exists:
                return path, writers
        return None

    @staticmethod
    def _source_location(
        source: Source, connections: dict[str, OneLakeConnection] | None
    ) -> tuple[str, str] | None:
        """Return a source's ``(type, location)`` for an existence probe.

        Connection-form sources are Delta tables at their resolved abfss://
        path; Delta sources use their alias, file sources their path.

        Returns:
            ``None`` when the source declares no probeable location.

        Raises:
            ConfigLocationError: If a connection-form source can't be resolved.
        """
        if source.connection:
            path = resolve_connection_path(
                source.connection, source.table, source.schema_override, connections
            )
            return "delta", path
        if source.type is None:
            return None
        path = source.alias if source.type == "delta" else source.path
        return (source.type, path) if path is not None else None

    def _source_location_exists(self, source_type: str, resolve_path: str) -> bool:
        """Job-free existence probe for a single source location.

        Existence, not readability: a location that exists but holds
        nothing parseable (an empty directory for a csv source, say)
        counts as present here, where the old reader-based probe failed
        schema inference and reported it missing. Deliberate — validate
        mode answers "is the config pointed at something real", and
        parse problems surface at execution with a better error. Same
        posture as the catalog-vs-Delta-readable gap documented on
        ``probe_delta_table_exists``.

        A failure to ask, such as a permission or storage error, raises
        rather than answering False, so it isn't reported as a missing
        source.
        """
        if source_type == "delta":
            return probe_delta_table_exists(self._spark, resolve_path)
        jvm: Any = self._spark._jvm  # noqa: SLF001
        jsc: Any = self._spark._jsc  # noqa: SLF001
        location = jvm.org.apache.hadoop.fs.Path(resolve_path)
        fs = location.getFileSystem(jsc.hadoopConfiguration())
        return bool(fs.exists(location))

    @staticmethod
    def _collect_all_threads(loaded: LoadedConfig) -> dict[str, Thread]:
        """Flatten all threads from a resolved config into a single dict."""
        if loaded.config_type == "thread":
            model = loaded.model
            if not isinstance(model, Thread):
                raise TypeError(f"Expected Thread, got {type(model).__name__}")
            return {loaded.config_name: model}

        result: dict[str, Thread] = {}
        for thread_map in loaded.document.weave_threads().values():
            result.update(thread_map)
        return result

    # ------------------------------------------------------------------
    # Plan mode
    # ------------------------------------------------------------------

    def _run_plan(
        self,
        loaded: LoadedConfig,
        *,
        tags: list[str] | None = None,
        thread_names: list[str] | None = None,
    ) -> RunResult:
        """Build and return execution plans without executing."""
        plans: list[Any] = []
        all_warnings: list[str] = []

        if loaded.config_type == "thread":
            return RunResult(
                status="success",
                mode=ExecutionMode.PLAN,
                config_type="thread",
                config_name=loaded.config_name,
                execution_plan=None,
            )

        if loaded.config_type == "weave":
            model = loaded.model
            if not isinstance(model, Weave):
                raise TypeError(f"Expected Weave, got {type(model).__name__}")
            wt = loaded.document.weave_threads().get(loaded.config_name, {})
            wt, warnings = self._filter_threads(wt, tags=tags, thread_names=thread_names)
            all_warnings.extend(warnings)
            if wt:
                filtered_entries = [e for e in model.threads if e.name in wt]
                plans.append(
                    build_plan(
                        weave_name=loaded.config_name,
                        threads=wt,
                        thread_entries=filtered_entries,
                        lookups=dict(model.lookups) if model.lookups else None,
                    )
                )

        elif loaded.config_type == "loom":
            model = loaded.model
            if not isinstance(model, Loom):
                raise TypeError(f"Expected Loom, got {type(model).__name__}")
            weave_models = loaded.document.weaves()
            weave_threads = loaded.document.weave_threads()
            for weave_entry in model.weaves:
                weave_name = weave_entry.effective_name
                weave = weave_models.get(weave_name)
                if weave is None:
                    continue
                wt = weave_threads.get(weave_name, {})
                wt, warnings = self._filter_threads(wt, tags=tags, thread_names=thread_names)
                all_warnings.extend(warnings)
                if wt:
                    filtered_entries = [e for e in weave.threads if e.name in wt]
                    plans.append(
                        build_plan(
                            weave_name=weave_name,
                            threads=wt,
                            thread_entries=filtered_entries,
                            lookups=dict(weave.lookups) if weave.lookups else None,
                        )
                    )

        result = RunResult(
            status="success",
            mode=ExecutionMode.PLAN,
            config_type=loaded.config_type,
            config_name=loaded.config_name,
            execution_plan=plans if plans else None,
            warnings=all_warnings,
        )

        # Thread models for explain() thread detail
        if loaded.config_type in ("weave", "loom"):
            result.thread_models = MappingProxyType(_thread_models(loaded))

        return result

    # ------------------------------------------------------------------
    # Preview mode
    # ------------------------------------------------------------------

    def _run_preview(
        self,
        loaded: LoadedConfig,
        *,
        tags: list[str] | None = None,
        thread_names: list[str] | None = None,
        sample_rows: int = 100,
    ) -> RunResult:
        """Execute with sampled data — no writes, no assertions."""
        import contextlib

        from weevr.operations.hashing import compute_keys
        from weevr.operations.pipeline import ObservationRegistry, run_pipeline
        from weevr.operations.readers import read_sources
        from weevr.operations.validation import validate_dataframe
        from weevr.operations.writers import apply_target_mapping

        all_threads = self._collect_all_threads(loaded)
        all_threads, warnings = self._filter_threads(
            all_threads, tags=tags, thread_names=thread_names
        )
        scopes = _thread_scopes(loaded)
        # Only the selected threads run, so only they produce tables here.
        producers = build_producer_map(all_threads)

        preview_data: dict[str, Any] = {}
        preview_metadata: dict[str, PreviewThreadMetadata] = {}
        errors: list[str] = []

        for thread_name, thread in all_threads.items():
            try:
                # Resolve steps get full lookup frames: matching sampled rows
                # against a sampled lookup would miss most valid keys.
                scope = scopes.get(thread_name)
                visible_lookups = registry_as_dict(scope.lookups) if scope is not None else None
                # Preview doesn't feed one thread's output into the next, so a
                # thread reading a table this run hasn't written yet has
                # nothing to read: report it as skipped, not as a failure.
                waiting = self._awaited_in_run_table(
                    thread_name, thread, visible_lookups or {}, producers
                )
                if waiting is not None:
                    path, writers = waiting
                    produced_by = ", ".join(f"thread '{w}'" for w in writers)
                    warnings.append(
                        f"Preview skipped thread '{thread_name}': reads {path}, produced by "
                        f"{produced_by} in this run; not materialized yet"
                    )
                    continue
                lookup_dfs, resolve_lookups = resolve_lookup_frames(
                    thread,
                    visible_lookups or {},
                    {},
                    self._spark,
                    connections=thread.connections,
                )
                read_dfs = read_sources(
                    self._spark,
                    {k: v for k, v in thread.sources.items() if k not in lookup_dfs},
                    connections=thread.connections,
                )
                frames = {**read_dfs, **lookup_dfs}
                sources_map = {key: frames[key].limit(sample_rows) for key in thread.sources}

                obs_registry = ObservationRegistry(scope="main")
                if thread.with_:
                    sources_map = resolve_with_block(
                        thread,
                        sources_map,
                        observations=obs_registry,
                        lookups=resolve_lookups or None,
                    )

                df = next(iter(sources_map.values()))
                df = run_pipeline(
                    df,
                    thread.steps,
                    sources_map,
                    lookups=resolve_lookups or None,
                    observations=obs_registry,
                )

                if thread.validations:
                    # Preview discards the per-rule results, so skip their
                    # aggregation. Consequence (chosen behavior): no fatal
                    # detection here — sampled data failing a fatal rule
                    # still renders the error-split view.
                    outcome = validate_dataframe(df, thread.validations, compute_results=False)
                    df = outcome.clean_df

                if thread.keys is not None:
                    df = compute_keys(df, thread.keys)

                df = apply_target_mapping(df, thread.target, self._spark)

                preview_data[thread_name] = df

                # Capture preview metadata for enhanced HTML rendering
                meta: PreviewThreadMetadata = {
                    "output_schema": [(name, str(dtype)) for name, dtype in df.dtypes],
                }
                with contextlib.suppress(Exception):
                    meta["samples"] = {"output": df.limit(10).toPandas().to_dict("records")}
                # Harvest step stats after the sampling action fulfilled the
                # observations. Sample-scoped by construction (no write in
                # preview). Deliberately OUTSIDE the samples suppress with
                # its own guard: a harvest failure warns and degrades — it
                # must never demote the thread to errors. Skipped entirely
                # when the sample capture failed: no action executed the
                # plan, so every observation would just time out.
                if "samples" in meta:
                    try:
                        step_stats = obs_registry.harvest()
                        if step_stats:
                            meta["step_stats"] = step_stats
                    except Exception:
                        logger.warning(
                            "Preview step-statistics harvest failed for thread '%s'",
                            thread_name,
                        )
                # Row count captured once here so rendering never has to
                # compute it. Rides its own suppress — a flaky count must
                # degrade to a missing key (renderers show "(unavailable)"),
                # never demote the whole thread to errors via the outer try.
                # Unlike the harvest above, deliberately NOT gated on the
                # sample capture: every metadata key degrades independently,
                # so a thread can render a count without a sample table.
                with contextlib.suppress(Exception):
                    meta["row_count"] = df.count()
                preview_metadata[thread_name] = meta
            except Exception as exc:
                errors.append(f"Preview failed for thread '{thread_name}': {exc}")

        status = "failure" if errors and not preview_data else "success"
        if errors and preview_data:
            status = "partial"

        result = RunResult(
            status=status,
            mode=ExecutionMode.PREVIEW,
            config_type=loaded.config_type,
            config_name=loaded.config_name,
            preview_data=preview_data if preview_data else None,
            warnings=warnings + errors,
        )
        result.preview_metadata = preview_metadata if preview_metadata else None
        result.thread_models = MappingProxyType(dict(all_threads))
        return result

    # ------------------------------------------------------------------
    # Config assembly
    # ------------------------------------------------------------------

    def _resolve_config_path(self, path: str | Path) -> ConfigLocation:
        """Resolve a config path relative to the project root.

        Args:
            path: Config file path, relative to the project root.

        Returns:
            A :class:`ConfigLocation` for the config file.
        """
        p = Path(path)
        if p.is_absolute():
            # Absolute paths are always local. A user supplying an absolute
            # filesystem path overrides the project root entirely — even in
            # a Tier 2 OneLake context — because there is no sensible way
            # to interpret a host-filesystem path against an abfss URI.
            return LocalConfigLocation(p)
        return self._project_root.join(str(path))

    def _load(self, path: str | Path) -> LoadedConfig:
        """Load a config through the shared loader, injecting this context's inputs.

        Resolves *path* against the project root and passes the context's
        params and Fabric context to :func:`weevr.config.loader.load_resolved_config`
        — the same load ``load_config`` and ``load_resolved`` run. Load warnings
        are logged here.
        """
        from weevr.config.fabric import build_fabric_context

        document, warnings = load_resolved_config(
            self._resolve_config_path(path),
            runtime_params=self._params,
            project_root=self._project_root,
            fabric_context=build_fabric_context(self._spark),
            project=self._project,
            strict_params=self._strict_params,
        )
        for warning in render_diagnostics(warnings):
            logger.warning(warning)
        loaded = LoadedConfig.from_document(document, warnings)
        self._remember_connections(loaded)
        return loaded

    def _remember_connections(self, loaded: LoadedConfig) -> None:
        """Keep the connections a loaded config declares for the Run Store.

        Only a config that declares connections replaces the registry, so loading a
        thread or weave afterwards doesn't clear what a loom declared.
        """
        connections = getattr(loaded.model, "connections", None)
        if connections:
            self._connections = connections

    def _plan_problems(self, path: str | Path) -> list[Diagnostic]:
        """Plan problems of the files of a config that do load, for a failed run."""
        loaded, _ = self._load_for_validate(path, log_warnings=False)
        if loaded is None:
            return []
        return [i.diagnostic for i in loaded.validate_plans() if i.diagnostic is not None]

    def _load_for_validate(
        self, path: str | Path, *, log_warnings: bool = True
    ) -> tuple[LoadedConfig | None, list[Diagnostic]]:
        """Load for validate mode: problems are returned, not raised.

        Returns:
            ``(loaded, errors)``: the config without the files that failed,
            or ``None`` when the file itself failed; and the error diagnostics.
        """
        from weevr.config.fabric import build_fabric_context

        document, warnings, errors = collect_resolved_config(
            self._resolve_config_path(path),
            runtime_params=self._params,
            project_root=self._project_root,
            fabric_context=build_fabric_context(self._spark),
            project=self._project,
            strict_params=self._strict_params,
        )
        if log_warnings:
            for warning in render_diagnostics(warnings):
                logger.warning(warning)
        if document is None:
            return None, errors
        loaded = LoadedConfig.from_document(document, warnings)
        self._remember_connections(loaded)
        return loaded, errors


def _display_kind(location: ConfigLocation) -> str:
    """The kind of a document that failed to load: declared, else by typed extension."""
    return declared_kind(location) or TYPED_EXTENSIONS.get(location.suffix.lower(), "")


def _thread_models(loaded: LoadedConfig) -> dict[str, Thread]:
    """Every thread model a loaded config holds, by effective name.

    A thread config gives itself; a weave its threads; a loom every weave's
    threads, a later weave's thread winning a name clash.
    """
    document = loaded.document
    if isinstance(document.model, Thread):
        return {document.name: document.model}
    merged: dict[str, Thread] = {}
    for threads in document.weave_threads().values():
        merged.update(threads)
    return merged


def _thread_scopes(loaded: LoadedConfig) -> dict[str, ScopeRegistries]:
    """Each thread's effective lookups, column sets and variables, by effective name.

    Read from the resolved config tree. A thread name shared across a loom's
    weaves takes the last weave's thread, matching :func:`_thread_models`.
    """
    document = loaded.document
    if document.kind == "thread":
        return {document.name: document.effective}
    weaves = document.children.values() if document.kind == "loom" else [document]
    return {name: thread.effective for weave in weaves for name, thread in weave.children.items()}
