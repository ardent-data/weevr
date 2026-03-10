"""Execution planner — builds a DAG-based execution plan from a weave's thread config."""

from __future__ import annotations

from collections import deque

from weevr.errors.exceptions import ConfigError
from weevr.model.base import FrozenBase
from weevr.model.lookup import Lookup
from weevr.model.thread import Thread
from weevr.model.weave import ThreadEntry


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


def _normalize_path(path: str) -> str:
    """Normalize a path for DAG matching.

    Strips trailing slashes and preserves case (DEC-003).
    """
    return path.rstrip("/")


def _extract_target_path(thread: Thread) -> str | None:
    """Return the resolved target path for a thread, normalized.

    Prefers ``target.alias`` over ``target.path`` to match executor behaviour.
    Returns ``None`` if neither is set.
    """
    raw = thread.target.alias or thread.target.path
    if raw is None:
        return None
    return _normalize_path(raw)


def _extract_source_paths(thread: Thread) -> set[str]:
    """Return all normalized source paths/aliases declared on a thread."""
    paths: set[str] = set()
    for source in thread.sources.values():
        if source.alias is not None:
            paths.add(_normalize_path(source.alias))
        if source.path is not None:
            paths.add(_normalize_path(source.path))
    return paths


def _build_target_index(threads: dict[str, Thread]) -> dict[str, str]:
    """Build a mapping of normalized target path to thread name.

    Used by the dependency graph builder and lookup dependency inference to
    identify which thread produces a given data path.

    Args:
        threads: Mapping of thread name to Thread config.

    Returns:
        A dict mapping normalized target path to the producing thread's name.
    """
    target_index: dict[str, str] = {}
    for name, thread in threads.items():
        path = _extract_target_path(thread)
        if path is not None:
            target_index[path] = name
    return target_index


def _build_dependency_graph(
    threads: dict[str, Thread],
    thread_entries: list[ThreadEntry],
    target_index: dict[str, str] | None = None,
) -> tuple[dict[str, list[str]], dict[str, list[str]]]:
    """Build inferred and explicit dependency dicts.

    For each thread, auto-infers an edge when another thread's target path
    appears in this thread's source paths. Also merges explicit dependencies
    declared on ThreadEntry objects.

    Args:
        threads: Mapping of thread name to Thread config.
        thread_entries: Ordered thread entries from the weave config.
        target_index: Pre-built target path index. Built internally when ``None``.

    Returns:
        A ``(inferred_deps, explicit_deps)`` tuple where each is a
        ``dict[thread_name, list[upstream_thread_name]]``.

    Raises:
        ConfigError: If a thread referenced in explicit dependencies does not
            exist in the ``threads`` dict.
    """
    if target_index is None:
        target_index = _build_target_index(threads)

    # Build explicit deps index from ThreadEntry list
    explicit_index: dict[str, list[str]] = {name: [] for name in threads}
    for entry in thread_entries:
        if entry.dependencies:
            for dep in entry.dependencies:
                if dep not in threads:
                    raise ConfigError(
                        f"Thread '{entry.name}' declares explicit dependency on '{dep}', "
                        f"but '{dep}' is not defined in the weave"
                    )
            explicit_index[entry.name] = list(entry.dependencies)

    # Infer dependencies from path matching
    inferred: dict[str, list[str]] = {name: [] for name in threads}
    for name, thread in threads.items():
        source_paths = _extract_source_paths(thread)
        for src_path in source_paths:
            producer = target_index.get(src_path)
            if producer is not None and producer != name and producer not in inferred[name]:
                inferred[name].append(producer)

    return inferred, explicit_index


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
        # Explicit cache=False suppresses auto-caching (EC-008)
        if thread.cache is False:
            continue
        if len(dependents.get(name, [])) >= 2:
            targets.append(name)
    return sorted(targets)


def _infer_lookup_dependencies(
    threads: dict[str, Thread],
    lookups: dict[str, Lookup],
    target_index: dict[str, str],
) -> tuple[dict[str, list[str]], dict[str, str | None]]:
    """Infer implicit thread dependencies mediated by lookups.

    When a lookup's source reads from a table produced by thread A, and
    thread B consumes that lookup, then B implicitly depends on A.

    Args:
        threads: Mapping of thread name to Thread config.
        lookups: Mapping of lookup name to Lookup config.
        target_index: Pre-built target path to thread name index.

    Returns:
        A tuple of ``(lookup_deps, lookup_producer)`` where:
        - ``lookup_deps`` maps consumer thread names to the list of producer
          thread names they implicitly depend on (via lookups).
        - ``lookup_producer`` maps lookup names to the producing thread name,
          or ``None`` for external lookups.
    """
    # Find the producer thread for each lookup
    lookup_producer: dict[str, str | None] = {}
    for lk_name, lk in lookups.items():
        src = lk.source
        raw = src.alias or src.path
        if raw is not None:
            normalized = _normalize_path(raw)
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

    # Build implicit deps: consumer depends on producer
    lookup_deps: dict[str, list[str]] = {name: [] for name in threads}
    for lk_name, producer in lookup_producer.items():
        if producer is None:
            continue
        for consumer in lookup_consumers.get(lk_name, []):
            if consumer != producer and producer not in lookup_deps[consumer]:
                lookup_deps[consumer].append(producer)

    return lookup_deps, lookup_producer


def _compute_lookup_schedule(
    lookups: dict[str, Lookup],
    lookup_producer: dict[str, str | None],
    execution_order: list[list[str]],
) -> dict[int, list[str]]:
    """Compute when each materialized lookup should be read.

    Lookups with no producer (external data) are scheduled at group 0 (before
    the first execution group). Lookups whose source is produced by a thread
    in group N are scheduled at group N+1 (after the producer completes).

    Args:
        lookups: Mapping of lookup name to Lookup config.
        lookup_producer: Maps lookup name to its producer thread, or ``None``.
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
            the message), or if an explicit dependency references a thread that
            does not exist in ``threads``.
    """
    thread_names = list(threads.keys())

    # 0. Build shared target index
    target_index = _build_target_index(threads)

    # 1. Build inferred and explicit dependency graphs
    inferred, explicit = _build_dependency_graph(threads, thread_entries, target_index)

    # 1b. Infer lookup-mediated dependencies when lookups are provided
    lookup_deps: dict[str, list[str]] | None = None
    lookup_producer: dict[str, str | None] | None = None
    if lookups:
        lookup_deps, lookup_producer = _infer_lookup_dependencies(threads, lookups, target_index)
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
        lookup_schedule = _compute_lookup_schedule(lookups, lookup_producer, execution_order)

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
    )
