# Telemetry API

The `weevr.telemetry` module provides structured logging, execution span
tracking, and the telemetry result types attached to a run
(`ThreadTelemetry`, `WeaveTelemetry`, `LoomTelemetry`, with their
validation, assertion, and export results). Telemetry settings cascade
through the loom/weave/thread hierarchy via `ExecutionConfig`.

::: weevr.telemetry
    options:
      members: true
      show_root_heading: true
      show_source: false
