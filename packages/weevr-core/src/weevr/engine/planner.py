"""Execution planner — builds a DAG-based execution plan from a weave's thread config."""

from __future__ import annotations

from collections import deque
from pathlib import Path
from typing import TYPE_CHECKING

from weevr.config.onelake import resolve_connection_path
from weevr.errors.exceptions import ConfigError
from weevr.model.base import FrozenBase
from weevr.model.lookup import Lookup
from weevr.model.thread import Thread
from weevr.model.weave import ThreadEntry

if TYPE_CHECKING:
    from weevr.engine.display import DAGDiagram


class ExecutionPlan(FrozenBase):
    """Immutable execution plan produced by the planner.

    Attributes:
        weave_name: Name of the weave this plan was built for.
        threads: All thread names in the weave.
        dependencies: Maps each thread to the list of threads it depends on.
        dependents: Maps each thread to the list of threads that depend on it.
        execution_order: Parallel execution groups in topological order.
            Each inner list contains threads that can run concurrently.
        cache_targets: Thread names whose output should be cached because
            they feed two or more downstream threads.
        inferred_dependencies: The auto-inferred subset of ``dependencies``,
            determined by matching target paths to source paths across threads.
        explicit_dependencies: The explicitly declared subset of ``dependencies``,
            sourced from ``ThreadEntry.dependencies`` in the weave config.
        lookup_schedule: Maps execution group indices to lookups that should
            be materialized at that boundary. Key 0 means before the first
            group; key N (N > 0) means after group N-1 finishes. ``None``
            when ``build_plan`` was called without lookups.
        lookup_producers: Maps lookup names to the thread that produces
            their source data, or ``None`` for external lookups. Only
            populated when ``build_plan`` was called with lookups.
        lookup_consumers: Maps lookup names to the list of threads that
            consume each lookup. Only populated when ``build_plan`` was
            called with lookups.
        conditional_target_writers: Maps a normalized target path to the
            writers sharing it under the condition exemption — every
            writer carries a thread-entry condition, and the conditions
            assert mutual exclusion (the runner enforces it at
            condition-resolution time). ``None`` when no target is
            shared this way. Lookups reading such a target have no
            single producer: ``lookup_producers`` records ``None`` for
            them, while the schedule places them after the last
            writer's group and consumers depend on every writer.
    """

    weave_name: str
    threads: list[str]
    dependencies: dict[str, list[str]]
    dependents: dict[str, list[str]]
    execution_order: list[list[str]]
    cache_targets: list[str]
    inferred_dependencies: dict[str, list[str]]
    explicit_dependencies: dict[str, list[str]]
    lookup_schedule: dict[int, list[str]] | None = None
    lookup_producers: dict[str, str | None] | None = None
    lookup_consumers: dict[str, list[str]] | None = None
    conditional_target_writers: dict[str, list[str]] | None = None

    def dag(self, *, dark: bool | None = None) -> DAGDiagram:
        """Return an inline SVG DAG diagram of this execution plan.

        The returned :class:`DAGDiagram` auto-renders in notebooks
        via ``_repr_svg_()`` and can be exported via ``save()``.

        Args:
            dark: Force dark (``True``) or light (``False``) palette. ``None``
                uses ``prefers-color-scheme`` media query (default).
        """
        from weevr.engine.display import DAGDiagram, render_dag_svg

        return DAGDiagram(render_dag_svg(self, dark=dark))

    def _repr_html_(self) -> str:
        """Notebook rich display protocol.

        Renders DAG visualization and dependency table for this plan.
        """
        from weevr.engine.display import render_execution_plan_html

        return render_execution_plan_html(self)


def _normalize_path(path: str) -> str:
    """Normalize a path for DAG matching.

    Strips trailing slashes and preserves case.
    """
    return path.rstrip("/")


def _extract_target_path(thread: Thread) -> str | None:
    """Return the resolved target path for a thread, normalized.

    Prefers ``target.alias`` over ``target.path`` to match executor
    behaviour; connection+table declarations resolve to their abfss://
    path so connection-form writers participate in dependency inference
    and the same-target conflict guard exactly like path-form writers.
    Returns ``None`` if nothing resolves.
    """
    raw = thread.target.alias or thread.target.path
    if raw is None and thread.target.connection:
        try:
            raw = resolve_connection_path(
                thread.target.connection,
                thread.target.table,
                thread.target.schema_override,
                thread.connections,
            )
        except ValueError:
            return None
    if raw is None:
        return None
    return _normalize_path(raw)


def _extract_source_paths(thread: Thread) -> set[str]:
    """Return all normalized source paths/aliases declared on a thread.

    Connection-declared sources resolve to their abfss:// path (matching
    the target side), so a connection-form reader of a connection-form
    writer's table gets a dependency edge. FUSE-vs-abfss notation for the
    same table cannot be unified here (needs a live session) — a known
    plan-time limitation.
    """
    paths: set[str] = set()
    for source in thread.sources.values():
        if source.alias is not None:
            paths.add(_normalize_path(source.alias))
        if source.path is not None:
            paths.add(_normalize_path(source.path))
        if source.connection is not None:
            try:
                paths.add(
                    _normalize_path(
                        resolve_connection_path(
                            source.connection,
                            source.table,
                            source.schema_override,
                            thread.connections,
                        )
                    )
                )
            except ValueError:
                continue
    return paths


def _build_explicit_index(
    threads: dict[str, Thread],
    thread_entries: list[ThreadEntry],
) -> dict[str, list[str]]:
    """Build the explicit dependency index from ThreadEntry declarations.

    Args:
        threads: Mapping of thread name to Thread config.
        thread_entries: Ordered thread entries from the weave config.

    Returns:
        A dict mapping each thread name to its explicitly declared upstream
        thread names (empty list when none are declared).

    Raises:
        ConfigError: If a thread referenced in explicit dependencies does not
            exist in the ``threads`` dict.
    """
    explicit_index: dict[str, list[str]] = {name: [] for name in threads}
    for entry in thread_entries:
        effective_name = entry.as_ or entry.name or (Path(entry.ref).stem if entry.ref else "")
        if entry.dependencies:
            for dep in entry.dependencies:
                if dep not in threads:
                    raise ConfigError(
                        f"Thread '{effective_name}' declares explicit dependency on '{dep}', "
                        f"but '{dep}' is not defined in the weave"
                    )
            explicit_index[effective_name] = list(entry.dependencies)
    return explicit_index


def _upstream_closure(start: str, explicit_index: dict[str, list[str]]) -> set[str]:
    """Return every thread reachable upstream from ``start`` via explicit deps."""
    seen: set[str] = set()
    stack = list(explicit_index.get(start, []))
    while stack:
        node = stack.pop()
        if node in seen:
            continue
        seen.add(node)
        stack.extend(explicit_index.get(node, []))
    return seen


def _resolve_writer_chain(
    writers: list[str],
    explicit_index: dict[str, list[str]],
) -> str | None:
    """Return the last writer if explicit dependencies totally order ``writers``.

    Writers are totally ordered when every pair is comparable via the
    transitive explicit-dependency relation (possibly through intermediate
    threads) and exactly one writer sits downstream of all the others.
    Returns ``None`` when the writers are not totally ordered.
    """
    closures = {w: _upstream_closure(w, explicit_index) for w in writers}
    for i, a in enumerate(writers):
        for b in writers[i + 1 :]:
            if a not in closures[b] and b not in closures[a]:
                return None
    terminals = [w for w in writers if all(o in closures[w] for o in writers if o != w)]
    if len(terminals) != 1:
        return None
    return terminals[0]


def _build_target_index(
    threads: dict[str, Thread],
    explicit_index: dict[str, list[str]],
    weave_name: str,
    conditioned: set[str],
) -> tuple[dict[str, str], dict[str, list[str]]]:
    """Build a mapping of normalized target path to producing thread name.

    Used by thread and lookup dependency inference to identify which thread
    produces a given data path.

    A target written by two or more threads is rejected unless one of two
    exemptions applies:

    - Explicit dependencies totally order the writers. The target then
      resolves to the last writer, so downstream consumers run after the
      target's final state exists.
    - Every writer carries a thread-entry condition. The conditions assert
      mutual exclusion (at most one writer survives per run); the runner
      enforces the assertion at condition-resolution time. Such a target
      has no single producer — it is returned in the conditional map
      instead of the index, and consumers depend on every writer.

    Unordered, uncondition-gated writers would land in one parallel group
    and race concurrent writes against a single table.

    Args:
        threads: Mapping of thread name to Thread config.
        explicit_index: Explicit dependency index, used to check whether
            same-target writers are serialized by declared dependencies.
        weave_name: Weave name for error messages.
        conditioned: Effective names of threads whose entry carries a
            condition.

    Returns:
        A tuple of ``(target_index, conditional_writers)``: the normalized
        target path → producing thread mapping, and the normalized target
        path → sorted writer names mapping for condition-exempted targets.

    Raises:
        ConfigError: If two or more threads write the same target without
            explicit dependencies totally ordering them and without every
            writer being condition-gated.
    """
    writers_by_path: dict[str, list[str]] = {}
    for name, thread in threads.items():
        path = _extract_target_path(thread)
        if path is not None:
            writers_by_path.setdefault(path, []).append(name)

    target_index: dict[str, str] = {}
    conditional_writers: dict[str, list[str]] = {}
    for path, writers in writers_by_path.items():
        if len(writers) == 1:
            target_index[path] = writers[0]
            continue
        last_writer = _resolve_writer_chain(writers, explicit_index)
        if last_writer is not None:
            target_index[path] = last_writer
            continue
        if all(w in conditioned for w in writers):
            conditional_writers[path] = sorted(writers)
            continue
        names = ", ".join(f"'{n}'" for n in sorted(writers))
        raise ConfigError(
            f"Threads {names} all write target '{path}' in weave "
            f"'{weave_name}' but are not ordered by explicit dependencies, "
            f"so concurrent writes would corrupt the target. Declare "
            f"explicit dependencies to serialize the writers, give each "
            f"thread its own target, or put a mutually exclusive condition "
            f"on every writer."
        )
    return target_index, conditional_writers


def _infer_dependencies(
    threads: dict[str, Thread],
    target_index: dict[str, str],
    conditional_writers: dict[str, list[str]],
) -> dict[str, list[str]]:
    """Build the inferred dependency dict from source/target path matching.

    For each thread, auto-infers an edge when another thread's target path
    appears in this thread's source paths. A source matching a
    condition-exempted shared target has no single producer — the consumer
    depends on EVERY writer, so it runs after whichever one survived.

    Args:
        threads: Mapping of thread name to Thread config.
        target_index: Pre-built target path index.
        conditional_writers: Condition-exempted shared targets and their
            writer sets.

    Returns:
        A ``dict[thread_name, list[upstream_thread_name]]`` of inferred edges.
    """
    inferred: dict[str, list[str]] = {name: [] for name in threads}
    for name, thread in threads.items():
        source_paths = _extract_source_paths(thread)
        for src_path in source_paths:
            producers = conditional_writers.get(src_path)
            if producers is None:
                single = target_index.get(src_path)
                producers = [single] if single is not None else []
            for producer in producers:
                if producer != name and producer not in inferred[name]:
                    inferred[name].append(producer)
    return inferred


def _merge_dependencies(
    inferred: dict[str, list[str]],
    explicit: dict[str, list[str]],
) -> dict[str, list[str]]:
    """Merge inferred and explicit dependency dicts into a unified graph."""
    merged: dict[str, list[str]] = {}
    for name in inferred:
        combined = list(inferred[name])
        for dep in explicit.get(name, []):
            if dep not in combined:
                combined.append(dep)
        merged[name] = combined
    return merged


def _build_dependents(
    thread_names: list[str],
    dependencies: dict[str, list[str]],
) -> dict[str, list[str]]:
    """Build the reverse of the dependency graph (thread → downstream threads)."""
    dependents: dict[str, list[str]] = {name: [] for name in thread_names}
    for name, deps in dependencies.items():
        for dep in deps:
            if name not in dependents[dep]:
                dependents[dep].append(name)
    return dependents


def _detect_cycles(
    thread_names: list[str],
    dependencies: dict[str, list[str]],
) -> list[str] | None:
    """Detect cycles in the dependency graph using DFS with white/gray/black colouring.

    Returns:
        The cycle path as a list of thread names if a cycle is found, or ``None``.
    """
    WHITE, GRAY, BLACK = 0, 1, 2
    colour: dict[str, int] = {name: WHITE for name in thread_names}
    path: list[str] = []

    def dfs(node: str) -> list[str] | None:
        colour[node] = GRAY
        path.append(node)
        for neighbour in dependencies.get(node, []):
            if colour[neighbour] == GRAY:
                # Found a back edge — extract the cycle segment
                cycle_start = path.index(neighbour)
                return path[cycle_start:] + [neighbour]
            if colour[neighbour] == WHITE:
                result = dfs(neighbour)
                if result is not None:
                    return result
        path.pop()
        colour[node] = BLACK
        return None

    for name in thread_names:
        if colour[name] == WHITE:
            result = dfs(name)
            if result is not None:
                return result
    return None


def _topological_sort(
    thread_names: list[str],
    dependencies: dict[str, list[str]],
) -> list[list[str]]:
    """Compute parallel execution groups using Kahn's algorithm.

    Each group (level) contains threads whose dependencies are all satisfied
    by threads in earlier groups — i.e., threads that can run concurrently.

    Returns:
        A list of groups, each group being a list of thread names.
    """
    in_degree: dict[str, int] = {name: 0 for name in thread_names}
    for name in thread_names:
        for _dep in dependencies.get(name, []):
            in_degree[name] += 1

    queue: deque[str] = deque(name for name in thread_names if in_degree[name] == 0)
    # Deterministic ordering within each level — sort for reproducibility
    current_level = sorted(queue)
    queue = deque(current_level)

    groups: list[list[str]] = []
    while queue:
        level = list(queue)
        groups.append(sorted(level))
        queue.clear()
        next_level: list[str] = []
        for node in level:
            # Find all downstream threads and decrement in-degree
            for candidate in thread_names:
                if node in dependencies.get(candidate, []):
                    in_degree[candidate] -= 1
                    if in_degree[candidate] == 0:
                        next_level.append(candidate)
        for name in sorted(next_level):
            queue.append(name)

    return groups


def _analyze_cache_targets(
    thread_names: list[str],
    threads: dict[str, Thread],
    dependents: dict[str, list[str]],
) -> list[str]:
    """Identify threads whose output should be cached.

    A thread is a cache target when its output feeds two or more downstream
    threads AND its ``cache`` field is not explicitly ``False``.

    Args:
        thread_names: All thread names in the weave.
        threads: Thread model objects (to check thread.cache override).
        dependents: Maps each thread to threads that depend on it.

    Returns:
        Sorted list of thread names that should be cached.
    """
    targets: list[str] = []
    for name in thread_names:
        thread = threads[name]
        # Explicit cache=False suppresses auto-caching
        if thread.cache is False:
            continue
        if len(dependents.get(name, [])) >= 2:
            targets.append(name)
    return sorted(targets)


def _infer_lookup_dependencies(
    threads: dict[str, Thread],
    lookups: dict[str, Lookup],
    target_index: dict[str, str],
    conditional_writers: dict[str, list[str]],
) -> tuple[
    dict[str, list[str]],
    dict[str, str | None],
    dict[str, list[str]],
    dict[str, list[str]],
]:
    """Infer implicit thread dependencies mediated by lookups.

    When a lookup's source reads from a table produced by thread A, and
    thread B consumes that lookup, then B implicitly depends on A. A lookup
    reading a condition-exempted shared target has no single producer:
    ``lookup_producer`` records ``None`` and the conditional map carries
    the full writer set — consumers depend on every writer, and the
    schedule places the lookup after the last writer's group.

    Args:
        threads: Mapping of thread name to Thread config.
        lookups: Mapping of lookup name to Lookup config.
        target_index: Pre-built target path to thread name index.
        conditional_writers: Condition-exempted shared targets and their
            writer sets.

    Returns:
        A tuple of ``(lookup_deps, lookup_producer, lookup_consumers,
        lookup_conditional)`` where:
        - ``lookup_deps`` maps consumer thread names to the list of producer
          thread names they implicitly depend on (via lookups).
        - ``lookup_producer`` maps lookup names to the producing thread name,
          or ``None`` for external lookups.
        - ``lookup_consumers`` maps lookup names to the list of thread names
          that consume each lookup.
        - ``lookup_conditional`` maps lookup names to the writer set of the
          conditional shared target they read (empty when they don't).
    """
    # Find the producer thread(s) for each lookup
    lookup_producer: dict[str, str | None] = {}
    lookup_conditional: dict[str, list[str]] = {}
    for lk_name, lk in lookups.items():
        src = lk.source
        raw = src.alias or src.path
        if raw is not None:
            normalized = _normalize_path(raw)
            writers = conditional_writers.get(normalized)
            if writers is not None:
                lookup_conditional[lk_name] = writers
                lookup_producer[lk_name] = None
            else:
                lookup_producer[lk_name] = target_index.get(normalized)
        else:
            lookup_producer[lk_name] = None

    # Find which threads consume each lookup
    lookup_consumers: dict[str, list[str]] = {lk: [] for lk in lookups}
    for thread_name, thread in threads.items():
        for source in thread.sources.values():
            if (
                source.lookup is not None
                and source.lookup in lookup_consumers
                and thread_name not in lookup_consumers[source.lookup]
            ):
                lookup_consumers[source.lookup].append(thread_name)

    # Build implicit deps: consumer depends on producer(s)
    lookup_deps: dict[str, list[str]] = {name: [] for name in threads}
    for lk_name in lookups:
        producers = lookup_conditional.get(lk_name)
        if producers is None:
            single = lookup_producer[lk_name]
            producers = [single] if single is not None else []
        for producer in producers:
            for consumer in lookup_consumers.get(lk_name, []):
                if consumer != producer and producer not in lookup_deps[consumer]:
                    lookup_deps[consumer].append(producer)

    return lookup_deps, lookup_producer, lookup_consumers, lookup_conditional


def _compute_lookup_schedule(
    lookups: dict[str, Lookup],
    lookup_producer: dict[str, str | None],
    lookup_conditional: dict[str, list[str]],
    execution_order: list[list[str]],
) -> dict[int, list[str]]:
    """Compute when each materialized lookup should be read.

    Lookups with no producer (external data) are scheduled at group 0 (before
    the first execution group). Lookups whose source is produced by a thread
    in group N are scheduled at group N+1 (after the producer completes).
    Lookups reading a condition-exempted shared target are scheduled after
    the LAST writer's group — the surviving writer may sit in any of them.

    Args:
        lookups: Mapping of lookup name to Lookup config.
        lookup_producer: Maps lookup name to its producer thread, or ``None``.
        lookup_conditional: Maps lookup names to the writer set of the
            conditional shared target they read.
        execution_order: Parallel execution groups from topological sort.

    Returns:
        A dict mapping group index to the list of lookup names that should
        be materialized before that group executes.
    """
    # Build thread → group index mapping
    thread_group_index: dict[str, int] = {}
    for idx, group in enumerate(execution_order):
        for name in group:
            thread_group_index[name] = idx

    schedule: dict[int, list[str]] = {}
    for lk_name, lk in lookups.items():
        if not lk.materialize:
            continue
        writers = lookup_conditional.get(lk_name)
        if writers:
            group_idx = max(thread_group_index[w] for w in writers) + 1
        else:
            producer = lookup_producer.get(lk_name)
            group_idx = 0 if producer is None else thread_group_index[producer] + 1
        schedule.setdefault(group_idx, []).append(lk_name)

    # Sort lookup names within each group for determinism
    for group_idx in schedule:
        schedule[group_idx] = sorted(schedule[group_idx])

    return schedule


def build_plan(
    weave_name: str,
    threads: dict[str, Thread],
    thread_entries: list[ThreadEntry],
    lookups: dict[str, Lookup] | None = None,
) -> ExecutionPlan:
    """Build an execution plan from a weave's threads and declared/inferred dependencies.

    Performs DAG construction, cycle detection, topological sort, and cache analysis
    to produce an immutable :class:`ExecutionPlan`.

    When ``lookups`` are provided, the planner also:

    - Infers implicit dependencies between threads mediated by lookups (a
      thread consuming a lookup whose source is produced by another thread
      depends on that producer).
    - Computes a **lookup schedule** mapping each execution group boundary to
      the lookups that should be materialized before that group runs.

    Args:
        weave_name: Name of the weave being planned.
        threads: Mapping of thread name to :class:`Thread` config.
        thread_entries: Ordered thread entries from the weave config, which may
            carry explicit dependency declarations.
        lookups: Optional weave-level lookup definitions. When provided, lookup-
            mediated dependencies are inferred and a materialization schedule is
            computed.

    Returns:
        An immutable :class:`ExecutionPlan`.

    Raises:
        ConfigError: If a circular dependency is detected (with cycle path in
            the message), if an explicit dependency references a thread that
            does not exist in ``threads``, or if two or more threads write the
            same target without explicit dependencies ordering them.
    """
    thread_names = list(threads.keys())

    # 0. Build the explicit dependency index first — the target index needs it
    #    to decide whether same-target writers are serialized. Conditioned
    #    effective names feed the condition exemption for shared targets.
    explicit = _build_explicit_index(threads, thread_entries)
    conditioned = {
        (entry.as_ or entry.name or (Path(entry.ref).stem if entry.ref else ""))
        for entry in thread_entries
        if entry.condition is not None
    }

    # 0b. Build shared target index (rejects unordered duplicate targets
    #     unless chained or fully condition-gated)
    target_index, conditional_writers = _build_target_index(
        threads, explicit, weave_name, conditioned
    )

    # 1. Infer data-based dependencies from source/target path matching
    inferred = _infer_dependencies(threads, target_index, conditional_writers)

    # 1b. Infer lookup-mediated dependencies when lookups are provided
    lookup_deps: dict[str, list[str]] | None = None
    lookup_producer: dict[str, str | None] | None = None
    lookup_consumers: dict[str, list[str]] | None = None
    lookup_conditional: dict[str, list[str]] = {}
    if lookups:
        lookup_deps, lookup_producer, lookup_consumers, lookup_conditional = (
            _infer_lookup_dependencies(threads, lookups, target_index, conditional_writers)
        )
        # Merge lookup deps into inferred deps
        for name, deps in lookup_deps.items():
            for dep in deps:
                if dep not in inferred[name]:
                    inferred[name].append(dep)

    # 2. Merge into unified dependency graph
    dependencies = _merge_dependencies(inferred, explicit)

    # 3. Detect cycles
    cycle = _detect_cycles(thread_names, dependencies)
    if cycle is not None:
        cycle_path = " → ".join(cycle)
        raise ConfigError(f"Circular dependency detected in weave '{weave_name}': {cycle_path}")

    # 4. Build reverse graph (dependents)
    dependents = _build_dependents(thread_names, dependencies)

    # 5. Compute parallel execution groups via topological sort
    execution_order = _topological_sort(thread_names, dependencies)

    # 6. Identify cache targets
    cache_targets = _analyze_cache_targets(thread_names, threads, dependents)

    # 7. Compute lookup materialization schedule
    lookup_schedule: dict[int, list[str]] | None = None
    if lookups and lookup_producer is not None:
        lookup_schedule = _compute_lookup_schedule(
            lookups, lookup_producer, lookup_conditional, execution_order
        )

    return ExecutionPlan(
        weave_name=weave_name,
        threads=thread_names,
        dependencies=dependencies,
        dependents=dependents,
        execution_order=execution_order,
        cache_targets=cache_targets,
        inferred_dependencies=inferred,
        explicit_dependencies=explicit,
        lookup_schedule=lookup_schedule,
        lookup_producers=lookup_producer,
        lookup_consumers=lookup_consumers,
        conditional_target_writers=conditional_writers or None,
    )
