# weevr

**weevr** is a metadata‑driven execution framework designed to translate declarative intent into repeatable, inspectable data processing workflows.

At a high level, weevr sits between *configuration* and *execution*. Rather than writing procedural code to orchestrate data movement and transformation, users describe **what** should happen using structured configuration. weevr then interprets that intent and coordinates the underlying execution engine to carry it out.

The initial focus of weevr is on Spark‑based data platforms (such as Microsoft Fabric), but the core ideas are intentionally platform‑agnostic.

---

## Core ideas

### Declarative over procedural

weevr does not generate code from configuration. Instead, configuration is **read at runtime** and drives execution decisions directly. The framework acts as a thin translation layer that maps declarative intent to concrete execution steps.

This approach emphasizes:

* Predictability over flexibility
* Explicit structure over ad‑hoc logic
* Inspectable artifacts over opaque execution

---

### Execution, not abstraction

weevr is not a replacement for Spark or a re‑implementation of data processing primitives. It intentionally relies on the native execution engine for all data movement, transformation, and optimization.

The role of weevr is to:

* Interpret configuration
* Determine execution order
* Enforce consistency and conventions
* Orchestrate calls into the underlying engine

---

### Planned conceptual model

While the implementation is still evolving, weevr is built around a small set of core concepts:

* **Threads** – individual, focused units of work
* **Weaves** – ordered compositions of threads
* **Looms** – execution contexts that coordinate one or more weaves
* **Projects** – the boundary for configuration, artifacts, and execution state

These concepts exist to provide a shared vocabulary for reasoning about execution without binding users to low‑level implementation details.

---

### Inspectable execution

A key design goal of weevr is that execution should leave behind **structured, inspectable artifacts**:

* Execution manifests
* Ordering and dependency resolution
* Materialized configuration snapshots
* Deterministic paths and outputs

This makes it possible to understand *what happened* and *why* without reverse‑engineering runtime behavior.

---

## Status

weevr is currently in an **early scaffolding and design phase**. The repository focuses on:

* Project structure and tooling
* CI, release, and contribution workflows
* Clarifying core concepts and boundaries

Functional capabilities will be introduced incrementally once the foundational design is settled.

---

## Non‑goals (by design)

* weevr is not a low‑code or no‑code platform
* weevr is not a visual workflow designer
* weevr does not attempt to hide the underlying execution engine

The intent is to reduce orchestration friction, not to obscure how data is actually processed.

---

## Contributing

See `CONTRIBUTING.md` for development setup, workflow expectations, and pull request conventions.

---

## License

This project is licensed under the terms of the `LICENSE` file in this repository.
