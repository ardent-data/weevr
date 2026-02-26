"""Context class — the user-facing entry point for weevr."""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from pydantic import ValidationError
from pyspark.sql import SparkSession

from weevr.config.inheritance import apply_inheritance
from weevr.config.parser import (
    detect_config_type_from_extension,
    extract_config_version,
    parse_yaml,
    validate_config_version,
)
from weevr.config.resolver import (
    build_param_context,
    resolve_references,
    resolve_variables,
)
from weevr.config.validation import validate_schema
from weevr.engine.executor import execute_thread
from weevr.engine.planner import build_plan
from weevr.engine.runner import execute_loom, execute_weave
from weevr.errors import ConfigError, DataValidationError, ExecutionError, ModelValidationError
from weevr.model.execution import LogLevel
from weevr.model.loom import Loom
from weevr.model.thread import Thread
from weevr.model.weave import Weave
from weevr.result import ExecutionMode, LoadedConfig, RunResult
from weevr.telemetry.logging import configure_logging

logger = logging.getLogger(__name__)


@dataclass
class _ResolvedConfig:
    """Intermediate container for resolved config data."""

    config_type: str
    config_name: str
    model: Thread | Weave | Loom
    weaves: dict[str, Weave] = field(default_factory=dict)
    threads: dict[str, dict[str, Thread]] = field(default_factory=dict)


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
        log_level: str = "standard",
        workspace: str | None = None,
        lakehouse: str | None = None,
    ) -> None:
        """Initialize a Context with a SparkSession and project reference."""
        if not isinstance(spark, SparkSession):
            raise TypeError(f"'spark' must be a SparkSession, got {type(spark).__name__}")

        try:
            resolved_level = LogLevel(log_level)
        except ValueError:
            valid = ", ".join(f"'{v.value}'" for v in LogLevel)
            raise ValueError(f"Invalid log_level '{log_level}'. Must be one of: {valid}") from None

        self._spark = spark
        self._params = params
        self._log_level = resolved_level
        self._project_root = self._resolve_project_path(project, workspace, lakehouse)
        configure_logging(self._log_level)

    @staticmethod
    def _resolve_project_path(
        project: str | Path,
        workspace: str | None,
        lakehouse: str | None,
    ) -> Path | str:
        """Resolve the project path using three-tier resolution.

        Args:
            project: Project name or direct path.
            workspace: Optional Fabric workspace ID.
            lakehouse: Optional Fabric lakehouse ID.

        Returns:
            Resolved project root path (Path for local, str for ABFS).

        Raises:
            ConfigError: If the project cannot be resolved.
        """
        project_str = str(project)

        # Tier 3: Direct path — absolute path or has .weevr suffix and exists
        if os.path.isabs(project_str) or (
            project_str.endswith(".weevr") and os.path.isdir(project_str)
        ):
            project_path = Path(project_str)
            if not project_str.endswith(".weevr"):
                raise ConfigError(
                    f"Project directory must have .weevr extension: {project_str}"
                )
            if not project_path.is_dir():
                raise ConfigError(
                    f"Project directory not found: {project_str}"
                )
            return project_path

        # Tier 2: OneLake qualified — workspace + lakehouse provided
        if workspace and lakehouse:
            return (
                f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com"
                f"/{lakehouse}/Files/{project_str}.weevr"
            )

        # Tier 1: Default lakehouse
        default_path = Path(f"/lakehouse/default/Files/{project_str}.weevr")
        if default_path.is_dir():
            return default_path

        raise ConfigError(
            "No default lakehouse available. Provide workspace and lakehouse "
            "parameters or a direct project path."
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
    def project_root(self) -> Path | str:
        """Resolved project root path."""
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
        resolved = self._load_resolved(path)
        return LoadedConfig(
            model=resolved.model,
            config_type=resolved.config_type,
            config_name=resolved.config_name,
            threads=resolved.threads or None,
            weaves=resolved.weaves or None,
        )

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
        resolved = self._load_resolved(path)

        try:
            if resolved_mode is ExecutionMode.EXECUTE:
                result = self._run_execute(resolved, tags=tags, thread_names=threads)
            elif resolved_mode is ExecutionMode.VALIDATE:
                result = self._run_validate(resolved)
            elif resolved_mode is ExecutionMode.PLAN:
                result = self._run_plan(resolved, tags=tags, thread_names=threads)
            else:
                result = self._run_preview(
                    resolved, tags=tags, thread_names=threads, sample_rows=sample_rows
                )
        except (ExecutionError, DataValidationError) as exc:
            duration_ms = (time.monotonic_ns() - start_ns) // 1_000_000
            return RunResult(
                status="failure",
                mode=resolved_mode,
                config_type=resolved.config_type,
                config_name=resolved.config_name,
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
        resolved: _ResolvedConfig,
        *,
        tags: list[str] | None = None,
        thread_names: list[str] | None = None,
    ) -> RunResult:
        """Execute a resolved config and wrap the result."""
        if resolved.config_type == "thread":
            model = resolved.model
            assert isinstance(model, Thread)
            engine_result = execute_thread(self._spark, model)
            assert engine_result.status in ("success", "failure")
            return RunResult(
                status=engine_result.status,  # type: ignore[arg-type]
                mode=ExecutionMode.EXECUTE,
                config_type="thread",
                config_name=resolved.config_name,
                detail=engine_result,
                telemetry=engine_result.telemetry,
            )

        if resolved.config_type == "weave":
            model = resolved.model
            assert isinstance(model, Weave)
            weave_threads = resolved.threads.get(resolved.config_name, {})
            weave_threads, warnings = self._filter_threads(
                weave_threads, tags=tags, thread_names=thread_names
            )

            if not weave_threads:
                return RunResult(
                    status="success",
                    mode=ExecutionMode.EXECUTE,
                    config_type="weave",
                    config_name=resolved.config_name,
                    warnings=warnings,
                )

            filtered_entries = [e for e in model.threads if e.name in weave_threads]
            plan = build_plan(
                weave_name=resolved.config_name,
                threads=weave_threads,
                thread_entries=filtered_entries,
            )
            engine_result = execute_weave(self._spark, plan, weave_threads)
            assert engine_result.status in ("success", "failure", "partial")
            return RunResult(
                status=engine_result.status,  # type: ignore[arg-type]
                mode=ExecutionMode.EXECUTE,
                config_type="weave",
                config_name=resolved.config_name,
                detail=engine_result,
                telemetry=engine_result.telemetry,
                warnings=warnings,
            )

        # loom
        model = resolved.model
        assert isinstance(model, Loom)
        all_warnings: list[str] = []

        filtered_weaves: dict[str, Weave] = {}
        filtered_threads: dict[str, dict[str, Thread]] = {}
        for weave_entry in model.weaves:
            weave_name = weave_entry.name
            weave = resolved.weaves.get(weave_name)
            if weave is None:
                continue
            wt = resolved.threads.get(weave_name, {})
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
                config_name=resolved.config_name,
                warnings=all_warnings,
            )

        engine_result = execute_loom(self._spark, model, filtered_weaves, filtered_threads)
        return RunResult(
            status=engine_result.status,
            mode=ExecutionMode.EXECUTE,
            config_type="loom",
            config_name=resolved.config_name,
            detail=engine_result,
            telemetry=engine_result.telemetry,
            warnings=all_warnings,
        )

    # ------------------------------------------------------------------
    # Thread filtering
    # ------------------------------------------------------------------

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
            assert tags is not None
            requested_tags = set(tags)
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

    def _run_validate(self, resolved: _ResolvedConfig) -> RunResult:
        """Validate config, DAG, and source existence without executing."""
        validation_errors: list[str] = []

        if resolved.config_type == "weave":
            model = resolved.model
            assert isinstance(model, Weave)
            wt = resolved.threads.get(resolved.config_name, {})
            try:
                build_plan(
                    weave_name=resolved.config_name,
                    threads=wt,
                    thread_entries=list(model.threads),
                )
            except Exception as exc:
                validation_errors.append(f"DAG validation failed: {exc}")

        elif resolved.config_type == "loom":
            model = resolved.model
            assert isinstance(model, Loom)
            for weave_entry in model.weaves:
                weave_name = weave_entry.name
                weave = resolved.weaves.get(weave_name)
                if weave is None:
                    validation_errors.append(f"Weave '{weave_name}' not found")
                    continue
                wt = resolved.threads.get(weave_name, {})
                try:
                    build_plan(
                        weave_name=weave_name,
                        threads=wt,
                        thread_entries=list(weave.threads),
                    )
                except Exception as exc:
                    validation_errors.append(
                        f"DAG validation failed for weave '{weave_name}': {exc}"
                    )

        all_threads = self._collect_all_threads(resolved)
        source_errors = self._check_source_existence(all_threads)
        validation_errors.extend(source_errors)

        status = "failure" if validation_errors else "success"
        return RunResult(
            status=status,
            mode=ExecutionMode.VALIDATE,
            config_type=resolved.config_type,
            config_name=resolved.config_name,
            validation_errors=validation_errors if validation_errors else None,
        )

    def _check_source_existence(self, threads: dict[str, Thread]) -> list[str]:
        """Check that all sources referenced by threads exist."""
        errors: list[str] = []
        checked: set[str] = set()

        for thread_name, thread in threads.items():
            for source_name, source in thread.sources.items():
                resolve_path = source.alias if source.type == "delta" else source.path
                if resolve_path is None or resolve_path in checked:
                    continue
                checked.add(resolve_path)

                try:
                    self._spark.read.format(
                        source.type if source.type != "excel" else "com.crealytics.spark.excel"
                    ).load(resolve_path).limit(0).collect()
                except Exception:
                    errors.append(
                        f"Source '{source_name}' in thread '{thread_name}' "
                        f"not found: {resolve_path}"
                    )
        return errors

    @staticmethod
    def _collect_all_threads(resolved: _ResolvedConfig) -> dict[str, Thread]:
        """Flatten all threads from a resolved config into a single dict."""
        if resolved.config_type == "thread":
            model = resolved.model
            assert isinstance(model, Thread)
            return {resolved.config_name: model}

        result: dict[str, Thread] = {}
        for thread_map in resolved.threads.values():
            result.update(thread_map)
        return result

    # ------------------------------------------------------------------
    # Plan mode
    # ------------------------------------------------------------------

    def _run_plan(
        self,
        resolved: _ResolvedConfig,
        *,
        tags: list[str] | None = None,
        thread_names: list[str] | None = None,
    ) -> RunResult:
        """Build and return execution plans without executing."""
        plans: list[Any] = []
        all_warnings: list[str] = []

        if resolved.config_type == "thread":
            return RunResult(
                status="success",
                mode=ExecutionMode.PLAN,
                config_type="thread",
                config_name=resolved.config_name,
                execution_plan=None,
            )

        if resolved.config_type == "weave":
            model = resolved.model
            assert isinstance(model, Weave)
            wt = resolved.threads.get(resolved.config_name, {})
            wt, warnings = self._filter_threads(wt, tags=tags, thread_names=thread_names)
            all_warnings.extend(warnings)
            if wt:
                filtered_entries = [e for e in model.threads if e.name in wt]
                plans.append(
                    build_plan(
                        weave_name=resolved.config_name,
                        threads=wt,
                        thread_entries=filtered_entries,
                    )
                )

        elif resolved.config_type == "loom":
            model = resolved.model
            assert isinstance(model, Loom)
            for weave_entry in model.weaves:
                weave_name = weave_entry.name
                weave = resolved.weaves.get(weave_name)
                if weave is None:
                    continue
                wt = resolved.threads.get(weave_name, {})
                wt, warnings = self._filter_threads(wt, tags=tags, thread_names=thread_names)
                all_warnings.extend(warnings)
                if wt:
                    filtered_entries = [e for e in weave.threads if e.name in wt]
                    plans.append(
                        build_plan(
                            weave_name=weave_name,
                            threads=wt,
                            thread_entries=filtered_entries,
                        )
                    )

        return RunResult(
            status="success",
            mode=ExecutionMode.PLAN,
            config_type=resolved.config_type,
            config_name=resolved.config_name,
            execution_plan=plans if plans else None,
            warnings=all_warnings,
        )

    # ------------------------------------------------------------------
    # Preview mode
    # ------------------------------------------------------------------

    def _run_preview(
        self,
        resolved: _ResolvedConfig,
        *,
        tags: list[str] | None = None,
        thread_names: list[str] | None = None,
        sample_rows: int = 100,
    ) -> RunResult:
        """Execute with sampled data — no writes, no assertions."""
        from weevr.operations.hashing import compute_keys
        from weevr.operations.pipeline import run_pipeline
        from weevr.operations.readers import read_sources
        from weevr.operations.validation import validate_dataframe
        from weevr.operations.writers import apply_target_mapping

        all_threads = self._collect_all_threads(resolved)
        all_threads, warnings = self._filter_threads(
            all_threads, tags=tags, thread_names=thread_names
        )

        preview_data: dict[str, Any] = {}
        errors: list[str] = []

        for thread_name, thread in all_threads.items():
            try:
                sources_map = read_sources(self._spark, thread.sources)
                for key in sources_map:
                    sources_map[key] = sources_map[key].limit(sample_rows)

                df = next(iter(sources_map.values()))
                df = run_pipeline(df, thread.steps, sources_map)

                if thread.validations:
                    outcome = validate_dataframe(df, thread.validations)
                    df = outcome.clean_df

                if thread.keys is not None:
                    df = compute_keys(df, thread.keys)

                df = apply_target_mapping(df, thread.target, self._spark)

                preview_data[thread_name] = df
            except Exception as exc:
                errors.append(f"Preview failed for thread '{thread_name}': {exc}")

        status = "failure" if errors and not preview_data else "success"
        if errors and preview_data:
            status = "partial"

        return RunResult(
            status=status,
            mode=ExecutionMode.PREVIEW,
            config_type=resolved.config_type,
            config_name=resolved.config_name,
            preview_data=preview_data if preview_data else None,
            warnings=warnings + errors,
        )

    # ------------------------------------------------------------------
    # Config assembly
    # ------------------------------------------------------------------

    def _resolve_config_path(self, path: str | Path) -> Path:
        """Resolve a config path relative to the project root.

        Args:
            path: Config file path, relative to the project root.

        Returns:
            Absolute path to the config file.
        """
        p = Path(path)
        if p.is_absolute():
            return p
        return Path(self._project_root) / p

    def _load_resolved(self, path: str | Path) -> _ResolvedConfig:
        """Run the config pipeline and return the hydrated model with children.

        Replicates the ``load_config`` pipeline but captures resolved child
        configs (``_resolved_threads`` / ``_resolved_weaves``) before Pydantic
        hydration strips them.
        """
        file_path = self._resolve_config_path(path)
        project_root = Path(self._project_root)

        # Steps 1-3: Parse, version, type
        raw = parse_yaml(file_path)
        version = extract_config_version(raw)

        ext_type = detect_config_type_from_extension(file_path)
        if ext_type is None:
            raise ConfigError(
                f"Unsupported extension '{file_path.suffix}'. "
                "Expected .thread, .weave, or .loom",
                file_path=str(file_path),
            )
        config_type = ext_type
        validate_config_version(version, config_type)

        # Step 4: Schema validation
        validated = validate_schema(raw, config_type)
        config_dict = validated.model_dump(exclude_unset=True)

        # Step 5: Parameter context
        param_context = build_param_context(
            self._params,
            config_dict.get("defaults") or config_dict.get("params"),
        )

        # Step 6: Variable resolution
        resolved = resolve_variables(config_dict, param_context)

        # Step 7: Resolve child references
        resolved_with_refs = resolve_references(
            resolved, config_type, project_root, self._params
        )

        # Step 8: Apply inheritance cascade
        if config_type == "loom" and "_resolved_weaves" in resolved_with_refs:
            loom_defaults = resolved_with_refs.get("defaults")
            for weave in resolved_with_refs["_resolved_weaves"]:
                if "_resolved_threads" in weave:
                    weave_defaults = weave.get("defaults")
                    for i, thread in enumerate(weave["_resolved_threads"]):
                        merged = apply_inheritance(loom_defaults, weave_defaults, thread)
                        weave["_resolved_threads"][i] = merged

        elif config_type == "weave" and "_resolved_threads" in resolved_with_refs:
            weave_defaults = resolved_with_refs.get("defaults")
            for i, thread in enumerate(resolved_with_refs["_resolved_threads"]):
                merged = apply_inheritance(None, weave_defaults, thread)
                resolved_with_refs["_resolved_threads"][i] = merged

        # Derive config name from stem and inject
        config_name = file_path.stem
        if not resolved_with_refs.get("name"):
            resolved_with_refs["name"] = config_name

        # Compute qualified key for top-level config
        try:
            rel = file_path.resolve().relative_to(project_root.resolve())
            resolved_with_refs["qualified_key"] = str(rel)
        except ValueError:
            resolved_with_refs["qualified_key"] = str(file_path)

        # Step 9: Hydrate top-level model
        model = self._hydrate_model(resolved_with_refs, config_type, file_path)

        # Extract and hydrate child models
        weaves: dict[str, Weave] = {}
        threads: dict[str, dict[str, Thread]] = {}

        if config_type == "weave":
            weave_name = config_name
            thread_map = self._hydrate_threads(
                resolved_with_refs.get("threads", []),
                resolved_with_refs.get("_resolved_threads", []),
                file_path,
            )
            threads[weave_name] = thread_map

        elif config_type == "loom":
            for weave_entry, weave_dict in zip(
                resolved_with_refs.get("weaves", []),
                resolved_with_refs.get("_resolved_weaves", []),
                strict=True,
            ):
                # Determine weave name from entry or resolved dict
                if isinstance(weave_entry, dict):
                    weave_name = weave_entry.get("name", "") or weave_entry.get("ref", "")
                    if weave_entry.get("ref") and not weave_entry.get("name"):
                        weave_name = Path(weave_entry["ref"]).stem
                else:
                    weave_name = str(weave_entry)

                if not weave_dict.get("name"):
                    weave_dict["name"] = weave_name
                weave_model = self._hydrate_model(weave_dict, "weave", file_path)
                if not isinstance(weave_model, Weave):  # pragma: no cover
                    continue
                weaves[weave_name] = weave_model

                thread_map = self._hydrate_threads(
                    weave_dict.get("threads", []),
                    weave_dict.get("_resolved_threads", []),
                    file_path,
                )
                threads[weave_name] = thread_map

        return _ResolvedConfig(
            config_type=config_type,
            config_name=config_name,
            model=model,
            weaves=weaves,
            threads=threads,
        )

    @staticmethod
    def _hydrate_model(
        data: dict[str, Any], config_type: str, source_path: Path
    ) -> Thread | Weave | Loom:
        """Hydrate a dict into a typed domain model."""
        model_map: dict[str, type[Thread | Weave | Loom]] = {
            "thread": Thread,
            "weave": Weave,
            "loom": Loom,
        }
        cls = model_map[config_type]
        try:
            return cls.model_validate(data)
        except ValidationError as exc:
            raise ModelValidationError(
                f"Model hydration failed for {config_type}: {exc}",
                cause=exc,
                file_path=str(source_path),
            ) from exc

    @staticmethod
    def _hydrate_threads(
        thread_entries: list[Any],
        resolved_dicts: list[dict[str, Any]],
        source_path: Path,
    ) -> dict[str, Thread]:
        """Hydrate resolved thread dicts into a name->Thread mapping."""
        result: dict[str, Thread] = {}
        for entry, thread_dict in zip(thread_entries, resolved_dicts, strict=True):
            # Determine the thread name from the entry or resolved dict
            if isinstance(entry, dict):
                name = entry.get("name", "") or entry.get("ref", "")
                if entry.get("ref") and not entry.get("name"):
                    name = Path(entry["ref"]).stem
            else:
                name = str(entry)

            if not thread_dict.get("name"):
                thread_dict["name"] = name
            try:
                result[name] = Thread.model_validate(thread_dict)
            except ValidationError as exc:
                raise ModelValidationError(
                    f"Thread hydration failed for '{name}': {exc}",
                    cause=exc,
                    file_path=str(source_path),
                ) from exc
        return result
