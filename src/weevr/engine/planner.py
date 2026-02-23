"""Execution planner — builds a DAG-based execution plan from a weave's thread config."""

from collections import deque

from weevr.errors.exceptions import ConfigError
from weevr.model.base import FrozenBase
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
    """

    weave_name: str
    threads: list[str]
    dependencies: dict[str, list[str]]
    dependents: dict[str, list[str]]
    execution_order: list[list[str]]
    cache_targets: list[str]
    inferred_dependencies: dict[str, list[str]]
    explicit_dependencies: dict[str, list[str]]


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


def _build_dependency_graph(
    threads: dict[str, Thread],
    thread_entries: list[ThreadEntry],
) -> tuple[dict[str, list[str]], dict[str, list[str]]]:
    """Build inferred and explicit dependency dicts.

    For each thread, auto-infers an edge when another thread's target path
    appears in this thread's source paths. Also merges explicit dependencies
    declared on ThreadEntry objects.

    Returns:
        A ``(inferred_deps, explicit_deps)`` tuple where each is a
        ``dict[thread_name, list[upstream_thread_name]]``.

    Raises:
        ConfigError: If a thread referenced in explicit dependencies does not
            exist in the ``threads`` dict.
    """
    # Build index: target_path → thread_name for fast lookup
    target_index: dict[str, str] = {}
    for name, thread in threads.items():
        path = _extract_target_path(thread)
        if path is not None:
            target_index[path] = name

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


def build_plan(
    weave_name: str,
    threads: dict[str, Thread],
    thread_entries: list[ThreadEntry],
) -> ExecutionPlan:
    """Build an execution plan from a weave's threads and declared/inferred dependencies.

    Performs DAG construction, cycle detection, topological sort, and cache analysis
    to produce an immutable :class:`ExecutionPlan`.

    Args:
        weave_name: Name of the weave being planned.
        threads: Mapping of thread name to :class:`Thread` config.
        thread_entries: Ordered thread entries from the weave config, which may
            carry explicit dependency declarations.

    Returns:
        An immutable :class:`ExecutionPlan`.

    Raises:
        ConfigError: If a circular dependency is detected (with cycle path in
            the message), or if an explicit dependency references a thread that
            does not exist in ``threads``.
    """
    thread_names = list(threads.keys())

    # 1. Build inferred and explicit dependency graphs
    inferred, explicit = _build_dependency_graph(threads, thread_entries)

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

    return ExecutionPlan(
        weave_name=weave_name,
        threads=thread_names,
        dependencies=dependencies,
        dependents=dependents,
        execution_order=execution_order,
        cache_targets=cache_targets,
        inferred_dependencies=inferred,
        explicit_dependencies=explicit,
    )
