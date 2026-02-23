"""Unified result types for the weevr Python API."""

from __future__ import annotations

from enum import StrEnum
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    from weevr.engine.planner import ExecutionPlan
    from weevr.engine.result import LoomResult, ThreadResult, WeaveResult
    from weevr.telemetry.results import LoomTelemetry, ThreadTelemetry, WeaveTelemetry


class ExecutionMode(StrEnum):
    """Execution modes for ``Context.run()``.

    Attributes:
        EXECUTE: Full execution — read, transform, write (default).
        VALIDATE: Config + DAG + source existence checks, no execution.
        PLAN: Build and return execution plans without executing.
        PREVIEW: Execute with sampled data, no writes.
    """

    EXECUTE = "execute"
    VALIDATE = "validate"
    PLAN = "plan"
    PREVIEW = "preview"


class RunResult:
    """Unified result returned by ``Context.run()`` for all execution modes.

    Provides a consistent interface regardless of config type (thread, weave,
    loom) or execution mode (execute, validate, plan, preview). Mode-specific
    fields are ``None`` when not applicable.

    Attributes:
        status: Aggregate outcome — ``"success"``, ``"failure"``, or ``"partial"``.
        mode: The execution mode that produced this result.
        config_type: Config kind that was executed (``"thread"``, ``"weave"``, ``"loom"``).
        config_name: Name derived from the config file path.
        duration_ms: Wall-clock duration in milliseconds.
        detail: Underlying engine result (execute mode only).
        telemetry: Structured telemetry data (execute mode only).
        execution_plan: Resolved execution plans (plan mode only).
        preview_data: Output DataFrames keyed by thread name (preview mode only).
        validation_errors: Error messages from validation checks (validate mode only).
        warnings: Non-fatal messages (e.g., zero threads matched a filter).
    """

    __slots__ = (
        "status",
        "mode",
        "config_type",
        "config_name",
        "duration_ms",
        "detail",
        "telemetry",
        "execution_plan",
        "preview_data",
        "validation_errors",
        "warnings",
    )

    def __init__(
        self,
        *,
        status: Literal["success", "failure", "partial"],
        mode: ExecutionMode,
        config_type: str,
        config_name: str,
        duration_ms: int = 0,
        detail: ThreadResult | WeaveResult | LoomResult | None = None,
        telemetry: ThreadTelemetry | WeaveTelemetry | LoomTelemetry | None = None,
        execution_plan: list[ExecutionPlan] | None = None,
        preview_data: dict[str, Any] | None = None,
        validation_errors: list[str] | None = None,
        warnings: list[str] | None = None,
    ) -> None:
        self.status = status
        self.mode = mode
        self.config_type = config_type
        self.config_name = config_name
        self.duration_ms = duration_ms
        self.detail = detail
        self.telemetry = telemetry
        self.execution_plan = execution_plan
        self.preview_data = preview_data
        self.validation_errors = validation_errors
        self.warnings: list[str] = warnings if warnings is not None else []

    def summary(self) -> str:
        """Return a formatted, human-readable execution summary."""
        return f"Status: {self.status}"


class LoadedConfig:
    """Wrapper around a hydrated config model returned by ``Context.load()``.

    Provides access to the underlying model (Thread, Weave, or Loom) and a
    lazily-built execution plan for weave/loom configs. Proxies attribute
    access to the model for convenience.

    Attributes:
        model: The hydrated domain model.
        config_type: Config kind (``"thread"``, ``"weave"``, ``"loom"``).
        config_name: Name derived from the config file path.
    """

    __slots__ = (
        "_model",
        "_config_type",
        "_config_name",
        "_threads",
        "_weaves",
        "_execution_plan",
        "_plan_built",
    )

    def __init__(
        self,
        model: Any,
        config_type: str,
        config_name: str,
        threads: dict[str, dict[str, Any]] | None = None,
        weaves: dict[str, Any] | None = None,
    ) -> None:
        self._model = model
        self._config_type = config_type
        self._config_name = config_name
        self._threads = threads
        self._weaves = weaves
        self._execution_plan: list[Any] | None = None
        self._plan_built = False

    @property
    def model(self) -> Any:
        """The underlying Thread, Weave, or Loom model."""
        return self._model

    @property
    def config_type(self) -> str:
        """Config kind: ``"thread"``, ``"weave"``, or ``"loom"``."""
        return self._config_type

    @property
    def config_name(self) -> str:
        """Name derived from the config file path."""
        return self._config_name

    @property
    def execution_plan(self) -> list[Any] | None:
        """Lazily-built execution plans for weave/loom configs.

        Returns ``None`` for thread configs. For weave configs, returns a
        single-element list. For loom configs, returns one plan per weave.
        """
        if self._plan_built:
            return self._execution_plan
        self._plan_built = True

        if self._config_type == "thread":
            self._execution_plan = None
            return self._execution_plan

        from weevr.engine.planner import build_plan
        from weevr.model.thread import Thread
        from weevr.model.weave import Weave

        plans: list[Any] = []

        if self._config_type == "weave" and self._threads is not None:
            weave = self._model
            weave_name = self._config_name
            thread_map = self._threads.get(weave_name, {})
            # Ensure Thread objects
            typed_threads: dict[str, Thread] = {}
            for name, t in thread_map.items():
                typed_threads[name] = t if isinstance(t, Thread) else Thread.model_validate(t)
            plans.append(
                build_plan(
                    weave_name=weave_name,
                    threads=typed_threads,
                    thread_entries=list(weave.threads),
                )
            )

        elif self._config_type == "loom" and self._weaves is not None and self._threads is not None:
            for weave_name in self._model.weaves:
                weave = self._weaves.get(weave_name)
                if weave is None:
                    continue
                weave_obj = weave if isinstance(weave, Weave) else Weave.model_validate(weave)
                thread_map = self._threads.get(weave_name, {})
                typed_threads = {}
                for name, t in thread_map.items():
                    typed_threads[name] = t if isinstance(t, Thread) else Thread.model_validate(t)
                plans.append(
                    build_plan(
                        weave_name=weave_name,
                        threads=typed_threads,
                        thread_entries=list(weave_obj.threads),
                    )
                )

        self._execution_plan = plans if plans else None
        return self._execution_plan

    def __getattr__(self, name: str) -> Any:
        """Proxy attribute access to the underlying model."""
        try:
            return getattr(self._model, name)
        except AttributeError:
            model_type = type(self._model).__name__
            raise AttributeError(
                f"'{type(self).__name__}' object has no attribute '{name}' "
                f"(underlying {model_type} model does not have this attribute either)"
            ) from None
