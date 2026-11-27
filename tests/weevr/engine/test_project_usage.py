"""Which project lookups and column sets a run's threads use.

A thread uses a project lookup or column set when it names it — in a
lookup-bound source, a resolve step or a rename step, in its main pipeline
or a ``with:`` sub-pipeline — and the name resolves to the project's
declaration rather than to one a lower scope declares.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from documents import TEST_PROJECT_ID, loom_body, thread_body, weave_body, write_named
from resolving import load_at
from weevr.config.document import ScopeRegistries
from weevr.engine.project_usage import (
    used_project_registries,
    without_unused_project_column_sets,
)


def _write_project(root: Path) -> None:
    root.mkdir(parents=True, exist_ok=True)
    body = {
        "kind": "project",
        "config_version": "2.0",
        "id": TEST_PROJECT_ID,
        "name": "test",
        "lookups": {
            "dim": {"source": {"type": "delta", "alias": "ref.dim"}},
            "other": {"source": {"type": "delta", "alias": "ref.other"}},
        },
        "column_sets": {
            "cs": {"source": {"type": "delta", "alias": "ref.cs"}},
            "spare": {"source": {"type": "delta", "alias": "ref.spare"}},
        },
    }
    (root / "weevr.yaml").write_text(yaml.safe_dump(body, sort_keys=False))


def _tree(
    root: Path,
    threads: dict[str, dict[str, Any]],
    *,
    weave: dict[str, Any] | None = None,
) -> Path:
    """A loom over weave ``w`` over the named threads; returns the loom."""
    _write_project(root)
    for name, extra in threads.items():
        write_named(root, f"{name}.thread", thread_body(alias=f"out.{name}", **extra))
    entries = [{"ref": f"{name}.thread"} for name in threads]
    write_named(root, "w.weave", weave_body(entries, **(weave or {})))
    return write_named(root, "l.loom", loom_body([{"ref": "w.weave"}]))


def _used(root: Path, threads: dict[str, dict[str, Any]], **kwargs: Any) -> ScopeRegistries:
    return used_project_registries(load_at(_tree(root, threads, **kwargs)).document)


SOURCE_LOOKUP = {"sources": {"s": {"lookup": "dim"}}}
RESOLVE = {"name": "dim_id", "lookup": "dim", "match": "k", "pk": "id"}
RENAME = {"rename": {"column_set": "cs"}}


class TestWhatCountsAsUse:
    def test_lookup_bound_source(self, tmp_path: Path) -> None:
        used = _used(tmp_path, {"t": SOURCE_LOOKUP})
        assert set(used.lookups or {}) == {"dim"}
        assert used.column_sets is None

    def test_resolve_step(self, tmp_path: Path) -> None:
        used = _used(tmp_path, {"t": {"steps": [{"resolve": RESOLVE}]}})
        assert set(used.lookups or {}) == {"dim"}

    def test_rename_step(self, tmp_path: Path) -> None:
        used = _used(tmp_path, {"t": {"steps": [RENAME]}})
        assert set(used.column_sets or {}) == {"cs"}
        assert used.lookups is None

    def test_with_sub_pipeline_resolve_and_rename(self, tmp_path: Path) -> None:
        sub = {"from": "s", "steps": [{"resolve": RESOLVE}, RENAME]}
        used = _used(tmp_path, {"t": {"with": {"sub": sub}}})
        assert set(used.lookups or {}) == {"dim"}
        assert set(used.column_sets or {}) == {"cs"}

    def test_nothing_named_is_nothing_used(self, tmp_path: Path) -> None:
        assert _used(tmp_path, {"t": {}}) == ScopeRegistries()


class TestShadowing:
    def test_a_weave_declaration_is_not_a_project_use(self, tmp_path: Path) -> None:
        weave = {"lookups": {"dim": {"source": {"type": "delta", "alias": "w.dim"}}}}
        assert _used(tmp_path, {"t": SOURCE_LOOKUP}, weave=weave).lookups is None

    def test_a_thread_declaration_is_not_a_project_use(self, tmp_path: Path) -> None:
        own = {"lookups": {"dim": {"source": {"type": "delta", "alias": "t.dim"}}}}
        assert _used(tmp_path, {"t": {**SOURCE_LOOKUP, **own}}).lookups is None


class TestThreadFilter:
    def test_only_running_threads_count(self, tmp_path: Path) -> None:
        loaded = load_at(_tree(tmp_path, {"a": SOURCE_LOOKUP, "b": {"steps": [RENAME]}}))
        all_threads = dict(loaded.document.children["w"].children)
        running = {"w": {"b": all_threads["b"].model}}

        used = used_project_registries(loaded.document, running)

        assert used.lookups is None
        assert set(used.column_sets or {}) == {"cs"}

    def test_none_counts_every_thread(self, tmp_path: Path) -> None:
        loaded = load_at(_tree(tmp_path, {"a": SOURCE_LOOKUP, "b": {"steps": [RENAME]}}))
        used = used_project_registries(loaded.document, None)
        assert set(used.lookups or {}) == {"dim"}
        assert set(used.column_sets or {}) == {"cs"}


class TestRoots:
    def test_standalone_thread(self, tmp_path: Path) -> None:
        _tree(tmp_path, {"t": SOURCE_LOOKUP})
        used = used_project_registries(load_at(tmp_path / "t.thread").document)
        assert set(used.lookups or {}) == {"dim"}

    def test_standalone_weave(self, tmp_path: Path) -> None:
        _tree(tmp_path, {"t": {"steps": [RENAME]}})
        used = used_project_registries(load_at(tmp_path / "w.weave").document)
        assert set(used.column_sets or {}) == {"cs"}

    def test_no_project_registries(self, tmp_path: Path) -> None:
        write_named(tmp_path, "t.thread", thread_body(sources=SOURCE_LOOKUP["sources"]))
        loaded = load_at(tmp_path / "t.thread")
        assert used_project_registries(loaded.document) == ScopeRegistries()


class TestWithoutUnusedProjectColumnSets:
    def test_drops_unused_project_sets_and_keeps_used(self, tmp_path: Path) -> None:
        loaded = load_at(_tree(tmp_path, {"t": {"steps": [RENAME]}}))
        document = loaded.document
        weave = document.children["w"]
        used = used_project_registries(document)

        kept = without_unused_project_column_sets(
            weave.effective.column_sets, document.project_scope, used
        )

        assert set(kept or {}) == {"cs"}

    def test_keeps_a_lower_declaration_of_a_project_name(self, tmp_path: Path) -> None:
        weave = {"column_sets": {"spare": {"source": {"type": "delta", "alias": "w.spare"}}}}
        loaded = load_at(_tree(tmp_path, {"t": {}}, weave=weave))
        document = loaded.document
        used = used_project_registries(document)

        kept = without_unused_project_column_sets(
            document.children["w"].effective.column_sets, document.project_scope, used
        )

        assert set(kept or {}) == {"spare"}

    def test_none_stays_none(self) -> None:
        assert without_unused_project_column_sets(None, None, ScopeRegistries()) is None
