"""Context class — the user-facing entry point for weevr."""

from __future__ import annotations

import logging
import os
import time
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession

from weevr.config.loader import load_resolved_config
from weevr.config.locations import ConfigLocation, LocalConfigLocation, RemoteConfigLocation
from weevr.config.onelake import resolve_connection_path
from weevr.delta import probe_delta_table_exists
from weevr.engine.executor import execute_thread, resolve_with_block
from weevr.engine.lookups import resolve_lookup_frames
from weevr.engine.planner import (
    build_plan,
    build_producer_map,
    find_in_run_producers,
    resolve_lookup_names,
)
from weevr.engine.resources import merge_resource_dicts
from weevr.engine.runner import execute_loom, execute_weave
from weevr.errors import (
    ConfigError,
    DataValidationError,
    ExecutionError,
)
from weevr.model.connection import OneLakeConnection
from weevr.model.execution import LogLevel, resolve_effective_execution
from weevr.model.lookup import Lookup
from weevr.model.loom import Loom
from weevr.model.source import Source
from weevr.model.thread import Thread
from weevr.model.weave import ConditionSpec, Weave
from weevr.result import ExecutionMode, LoadedConfig, PreviewThreadMetadata, RunResult
from weevr.telemetry.logging import configure_logging

logger = logging.getLogger(__name__)


class Context:
    """Entry point for all weevr operations.

    Wraps a SparkSession with a project reference, resolved parameters,
    and execution configuration. Provides ``run()`` for execution and
    ``load()`` for model inspection.

    Args:
        spark: Active SparkSession (required).
        project: Project identifier. Accepts three forms:

            - **Simple name** (e.g., ``"my_project"``): resolved via the
              default lakehouse at ``/lakehouse/default/Files/<name>.weevr/``.
            - **Qualified** (with *workspace* and *lakehouse*): resolved via
              OneLake ABFS path.
            - **Direct path** (e.g., ``"/path/to/project.weevr"``): used as-is.
        params: Runtime parameter overrides.
        log_level: Logging verbosity — ``"minimal"``, ``"standard"`` (default),
            ``"verbose"``, or ``"debug"``.
        workspace: Fabric workspace ID for cross-lakehouse resolution.
        lakehouse: Fabric lakehouse ID for cross-lakehouse resolution.
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
    ) -> None:
        """Initialize a Context with a SparkSession and project reference."""
        if not isinstance(spark, SparkSession):
            raise TypeError(f"'spark' must be a SparkSession, got {type(spark).__name__}")

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
        self._log_level = resolved_level
        self._project_root = self._resolve_project_path(project, workspace, lakehouse)
        configure_logging(self._log_level)

    def _resolve_project_path(
        self,
        project: str | Path,
        workspace: str | None,
        lakehouse: str | None,
    ) -> ConfigLocation:
        """Resolve the project path using three-tier resolution.

        Args:
            project: Project name or direct path.
            workspace: Optional Fabric workspace ID.
            lakehouse: Optional Fabric lakehouse ID.

        Returns:
            A :class:`ConfigLocation` for the project root. Tier 1 and Tier 3
            return a :class:`LocalConfigLocation`; Tier 2 returns a
            :class:`RemoteConfigLocation` bound to ``self._spark``.

        Raises:
            ConfigError: If the project cannot be loaded.
        """
        project_str = str(project)

        # Tier 3: Direct path — absolute path or has .weevr suffix and exists
        if os.path.isabs(project_str) or (
            project_str.endswith(".weevr") and os.path.isdir(project_str)
        ):
            project_path = Path(project_str)
            if not project_str.endswith(".weevr"):
                raise ConfigError(f"Project directory must have .weevr extension: {project_str}")
            if not project_path.is_dir():
                raise ConfigError(f"Project directory not found: {project_str}")
            return LocalConfigLocation(project_path)

        # Normalize: strip .weevr suffix if already present so Tier 1/2
        # don't produce a double extension (e.g. "my_project.weevr.weevr").
        base_name = project_str.removesuffix(".weevr")

        # Tier 2: OneLake qualified — workspace + lakehouse provided
        if workspace and lakehouse:
            uri = (
                f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com"
                f"/{lakehouse}/Files/{base_name}.weevr"
            )
            return RemoteConfigLocation(uri, self._spark)

        # Tier 1: Default lakehouse
        default_path = Path(f"/lakehouse/default/Files/{base_name}.weevr")
        if default_path.is_dir():
            return LocalConfigLocation(default_path)

        raise ConfigError(
            f"Project directory not found at {default_path}. "
            "Provide workspace and lakehouse parameters or a direct project path."
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
            A :class:`RunResult` describing the outcome.

        Raises:
            ValueError: If *mode* is invalid or both *tags* and *threads*
                are provided.
        """
        try:
            resolved_mode = ExecutionMode(mode)
        except ValueError:
            valid = ", ".join(f"'{m.value}'" for m in ExecutionMode)
            raise ValueError(f"Invalid mode '{mode}'. Must be one of: {valid}") from None

        if tags is not None and threads is not None:
            raise ValueError("'tags' and 'threads' are mutually exclusive")

        start_ns = time.monotonic_ns()
        loaded = self._load(path)

        # Single application point for the effective YAML log level —
        # before mode dispatch, so all four modes behave identically.
        self._apply_effective_log_level(loaded)

        try:
            if resolved_mode is ExecutionMode.EXECUTE:
                result = self._run_execute(loaded, tags=tags, thread_names=threads)
            elif resolved_mode is ExecutionMode.VALIDATE:
                result = self._run_validate(loaded)
            elif resolved_mode is ExecutionMode.PLAN:
                result = self._run_plan(loaded, tags=tags, thread_names=threads)
            else:
                result = self._run_preview(
                    loaded, tags=tags, thread_names=threads, sample_rows=sample_rows
                )
        except (ExecutionError, DataValidationError) as exc:
            duration_ms = (time.monotonic_ns() - start_ns) // 1_000_000
            return RunResult(
                status="failure",
                mode=resolved_mode,
                config_type=loaded.config_type,
                config_name=loaded.config_name,
                duration_ms=duration_ms,
                warnings=[str(exc)],
            )

        result.duration_ms = (time.monotonic_ns() - start_ns) // 1_000_000
        return result

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
            result._resolved_threads = {loaded.config_name: model}
            return result

        if loaded.config_type == "weave":
            model = loaded.model
            if not isinstance(model, Weave):
                raise TypeError(f"Expected Weave, got {type(model).__name__}")
            weave_threads = loaded.resolved_threads.get(loaded.config_name, {})
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
                variables=dict(model.variables) if model.variables else None,
                column_set_defs=dict(model.column_sets) if model.column_sets else None,
                connections=dict(model.connections) if model.connections else None,
                execution=standalone_execution,
                weave_name=loaded.config_name,
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
            result._resolved_threads = dict(weave_threads)
            return result

        # loom
        model = loaded.model
        if not isinstance(model, Loom):
            raise TypeError(f"Expected Loom, got {type(model).__name__}")
        all_warnings: list[str] = []

        filtered_weaves: dict[str, Weave] = {}
        filtered_threads: dict[str, dict[str, Thread]] = {}
        for weave_entry in model.weaves:
            weave_name = weave_entry.effective_name
            weave = loaded.resolved_weaves.get(weave_name)
            if weave is None:
                continue
            wt = loaded.resolved_threads.get(weave_name, {})
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
        result._resolved_threads = merged_threads
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

    def _run_validate(self, loaded: LoadedConfig) -> RunResult:
        """Validate config, DAG, and source existence without executing."""
        validation_errors: list[str] = []

        for issue in loaded.validate_plans():
            if issue.error is None:
                validation_errors.append(issue.message)
            elif loaded.config_type == "weave":
                validation_errors.append(f"DAG validation failed: {issue.message}")
            else:
                validation_errors.append(
                    f"DAG validation failed for weave '{issue.weave_name}': {issue.message}"
                )

        all_threads = self._collect_all_threads(loaded)
        source_errors = self._check_source_existence(all_threads)
        validation_errors.extend(source_errors)

        status = "failure" if validation_errors else "success"
        return RunResult(
            status=status,
            mode=ExecutionMode.VALIDATE,
            config_type=loaded.config_type,
            config_name=loaded.config_name,
            validation_errors=validation_errors if validation_errors else None,
        )

    def _check_source_existence(self, threads: dict[str, Thread]) -> list[str]:
        """Check that all sources referenced by threads exist.

        Existence is a metadata question: Delta sources, including
        connection-form ones, go through the catalog or Delta-log check,
        file sources through a filesystem probe — no reads, no schema
        inference, no Spark jobs.
        """
        errors: list[str] = []
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
                except ValueError as exc:
                    errors.append(f"Source '{source_name}' in thread '{thread_name}': {exc}")
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
                        f"Source '{source_name}' in thread '{thread_name}' "
                        f"could not be verified: {resolve_path}"
                    )
                    continue
                if not exists:
                    errors.append(
                        f"Source '{source_name}' in thread '{thread_name}' "
                        f"not found: {resolve_path}"
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
            except Exception:
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
            ValueError: If a connection-form source can't be resolved.
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
        for thread_map in loaded.resolved_threads.values():
            result.update(thread_map)
        return result

    @staticmethod
    def _inherited_lookups(loaded: LoadedConfig) -> dict[str, dict[str, Lookup]]:
        """Map each thread name to the lookups its weave and loom define.

        Weave lookups win over loom lookups, as they do at execution. A
        thread name shared across weaves takes the last weave's lookups,
        matching :meth:`_collect_all_threads`.
        """
        loom_lookups: dict[str, Lookup] | None = None
        if isinstance(loaded.model, Loom) and loaded.model.lookups:
            loom_lookups = dict(loaded.model.lookups)

        result: dict[str, dict[str, Lookup]] = {}
        for weave_name, threads in loaded.resolved_threads.items():
            weave = loaded.model if isinstance(loaded.model, Weave) else None
            if isinstance(loaded.model, Loom):
                weave = loaded.resolved_weaves.get(weave_name)
            weave_lookups = dict(weave.lookups) if weave and weave.lookups else None
            merged = merge_resource_dicts(loom_lookups, weave_lookups, "lookup", "loom", "weave")
            for thread_name in threads:
                result[thread_name] = merged or {}
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
            wt = loaded.resolved_threads.get(loaded.config_name, {})
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
            for weave_entry in model.weaves:
                weave_name = weave_entry.effective_name
                weave = loaded.resolved_weaves.get(weave_name)
                if weave is None:
                    continue
                wt = loaded.resolved_threads.get(weave_name, {})
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

        # Populate resolved threads for explain() thread detail
        if loaded.config_type == "weave":
            wt = loaded.resolved_threads.get(loaded.config_name, {})
            result._resolved_threads = dict(wt)
        elif loaded.config_type == "loom":
            merged: dict[str, Any] = {}
            for thread_map in loaded.resolved_threads.values():
                merged.update(thread_map)
            result._resolved_threads = merged

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
        inherited_lookups = self._inherited_lookups(loaded)
        # Only the selected threads run, so only they produce tables here.
        producers = build_producer_map(all_threads)

        preview_data: dict[str, Any] = {}
        preview_metadata: dict[str, PreviewThreadMetadata] = {}
        errors: list[str] = []

        for thread_name, thread in all_threads.items():
            try:
                # Resolve steps get full lookup frames: matching sampled rows
                # against a sampled lookup would miss most valid keys.
                visible_lookups = merge_resource_dicts(
                    inherited_lookups.get(thread_name),
                    dict(thread.lookups) if thread.lookups else None,
                    "lookup",
                    "weave",
                    "thread",
                )
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
        result._preview_metadata = preview_metadata if preview_metadata else None
        result._resolved_threads = dict(all_threads)
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

        hydrated = load_resolved_config(
            self._resolve_config_path(path),
            runtime_params=self._params,
            project_root=self._project_root,
            fabric_context=build_fabric_context(self._spark),
        )
        for warning in hydrated.warnings:
            logger.warning(warning)
        return LoadedConfig.from_hydrated(hydrated)
