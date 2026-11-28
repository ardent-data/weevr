"""Project lookups and column sets at run time.

``weevr.yaml`` declares a lookup ``dim`` (``materialize: true``), a lookup
``unused`` and a column set ``unused_cs`` that point at tables that don't
exist, and a column set ``cs``. Under a loom, a project lookup some thread
uses materializes once at loom start, as a loom lookup does; a standalone
weave materializes it at weave level. A project lookup or column set no
thread uses is never read, and a lower declaration of the same name is read
instead of the project's.
"""

from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any

import pytest
import yaml
from pyspark.sql import SparkSession

import weevr.context as context
from documents import project_yaml, write_document

pytestmark = pytest.mark.spark


def _lookup(path: Path, *, materialize: bool = True) -> dict[str, Any]:
    return {"source": {"type": "delta", "alias": str(path)}, "materialize": materialize}


def _lookup_thread(name: str, out: Path, **extra: Any) -> dict[str, Any]:
    return {
        "config_version": "2.0",
        "name": name,
        "sources": {"ref": {"lookup": "dim"}},
        "target": {"path": str(out)},
        "write": {"mode": "overwrite"},
        **extra,
    }


def _rename_thread(name: str, raw: Path, out: Path) -> dict[str, Any]:
    return {
        "config_version": "2.0",
        "name": name,
        "sources": {"raw": {"type": "delta", "alias": str(raw)}},
        "steps": [{"rename": {"column_set": "cs"}}],
        "target": {"path": str(out)},
        "write": {"mode": "overwrite"},
    }


@pytest.fixture
def project(spark: SparkSession, tmp_path: Path) -> dict[str, Any]:
    tables = tmp_path / "tables"
    for name, label in (("project_ref", "project"), ("weave_ref", "weave")):
        spark.createDataFrame([(1, label)], ["key", "label"]).write.format("delta").save(
            str(tables / name)
        )
    spark.createDataFrame([(1,)], ["a"]).write.format("delta").save(str(tables / "raw"))
    root = tmp_path / "proj.weevr"
    (root / "threads").mkdir(parents=True)
    (root / "maps").mkdir()
    (root / "maps" / "cs.yaml").write_text(
        yaml.safe_dump([{"source_name": "a", "target_name": "b"}])
    )
    blocks = {
        "lookups": {
            "dim": _lookup(tables / "project_ref"),
            "unused": _lookup(tables / "missing"),
        },
        "column_sets": {
            "cs": {"source": {"type": "yaml", "path": "maps/cs.yaml"}},
            "unused_cs": {"source": {"type": "delta", "alias": str(tables / "missing_cs")}},
        },
    }
    (root / "weevr.yaml").write_text(project_yaml() + yaml.safe_dump(blocks, sort_keys=False))
    return {"root": root, "tables": tables}


def _write(path: Path, body: dict[str, Any]) -> None:
    write_document(path, yaml.safe_dump(body, sort_keys=False))


def _weave(root: Path, name: str, threads: list[str], **extra: Any) -> None:
    body = {"config_version": "2.0", "threads": [{"ref": f"threads/{t}"} for t in threads]}
    _write(root / f"{name}.weave", {**body, **extra})


def _loom(root: Path, weaves: list[str], **extra: Any) -> None:
    body = {"config_version": "2.0", "weaves": [{"ref": f"{w}.weave"} for w in weaves]}
    _write(root / "l.loom", {**body, **extra})


def _labels(spark: SparkSession, path: Path) -> list[str]:
    return [row.label for row in spark.read.format("delta").load(str(path)).collect()]


@pytest.fixture
def spies(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Record each lookup and column set read, by scope and name."""
    import weevr.engine.executor as executor
    import weevr.engine.runner as runner
    from weevr.engine.column_sets import materialize_column_sets
    from weevr.engine.lookups import cleanup_lookups, materialize_lookups

    lookups: Counter[tuple[str, str]] = Counter()
    column_sets: Counter[tuple[str, str]] = Counter()
    cleaned: list[set[str]] = []

    def spy_lookups(scope: str) -> Any:
        def wrapper(spark_: SparkSession, defs: dict[str, Any], *args: Any, **kwargs: Any):
            for name, lookup in defs.items():
                if lookup.materialize:
                    lookups[(scope, name)] += 1
            return materialize_lookups(spark_, defs, *args, **kwargs)

        return wrapper

    def spy_column_sets(scope: str) -> Any:
        def wrapper(spark_: SparkSession, defs: dict[str, Any], *args: Any, **kwargs: Any):
            for name in defs:
                column_sets[(scope, name)] += 1
            return materialize_column_sets(spark_, defs, *args, **kwargs)

        return wrapper

    def spy_cleanup(cached: dict[str, Any]) -> None:
        cleaned.append(set(cached))
        cleanup_lookups(cached)

    monkeypatch.setattr(runner, "materialize_lookups", spy_lookups("runner"))
    monkeypatch.setattr(executor, "materialize_lookups", spy_lookups("thread"))
    monkeypatch.setattr(runner, "materialize_column_sets", spy_column_sets("runner"))
    monkeypatch.setattr(executor, "materialize_column_sets", spy_column_sets("thread"))
    monkeypatch.setattr(context, "materialize_column_sets", spy_column_sets("context"))
    monkeypatch.setattr(runner, "cleanup_lookups", spy_cleanup)
    return {"lookups": lookups, "column_sets": column_sets, "cleaned": cleaned}


class TestUnderALoom:
    """A used project lookup materializes once per loom run; unused ones are never read."""

    def test_two_weaves_share_one_read(
        self, spark: SparkSession, project: dict[str, Any], spies: dict[str, Any]
    ) -> None:
        root, tables = project["root"], project["tables"]
        _write(root / "threads" / "t1.thread", _lookup_thread("t1", tables / "out_t1"))
        _write(root / "threads" / "t2.thread", _lookup_thread("t2", tables / "out_t2"))
        _weave(root, "w1", ["t1.thread"])
        _weave(root, "w2", ["t2.thread"])
        _loom(root, ["w1", "w2"])

        result = context.Context(spark, str(root)).run("l.loom")

        assert result.status == "success"
        assert _labels(spark, tables / "out_t1") == ["project"]
        assert _labels(spark, tables / "out_t2") == ["project"]
        assert spies["lookups"] == Counter({("runner", "dim"): 1})
        # The loom owns the frame: cleaned at loom end, not by either weave.
        assert [names for names in spies["cleaned"] if "dim" in names] == [{"dim"}]

    def test_a_weave_shadowing_it_reads_its_own(
        self, spark: SparkSession, project: dict[str, Any], spies: dict[str, Any]
    ) -> None:
        root, tables = project["root"], project["tables"]
        _write(root / "threads" / "t1.thread", _lookup_thread("t1", tables / "out_t1"))
        _write(root / "threads" / "t2.thread", _lookup_thread("t2", tables / "out_t2"))
        _weave(root, "w1", ["t1.thread"], lookups={"dim": _lookup(tables / "weave_ref")})
        _weave(root, "w2", ["t2.thread"])
        _loom(root, ["w1", "w2"])

        result = context.Context(spark, str(root)).run("l.loom")

        assert result.status == "success"
        assert _labels(spark, tables / "out_t1") == ["weave"]
        assert _labels(spark, tables / "out_t2") == ["project"]
        assert spies["lookups"] == Counter({("runner", "dim"): 2})

    def test_shadowed_everywhere_is_never_read(
        self, spark: SparkSession, project: dict[str, Any], spies: dict[str, Any]
    ) -> None:
        root, tables = project["root"], project["tables"]
        _write(root / "threads" / "t1.thread", _lookup_thread("t1", tables / "out_t1"))
        _weave(root, "w1", ["t1.thread"])
        _loom(root, ["w1"], lookups={"dim": _lookup(tables / "weave_ref")})

        result = context.Context(spark, str(root)).run("l.loom")

        assert result.status == "success"
        assert _labels(spark, tables / "out_t1") == ["weave"]
        # The loom's own 'dim' once; the project's never.
        assert spies["lookups"] == Counter({("runner", "dim"): 1})

    def test_thread_shadowing_it_reads_its_own(
        self, spark: SparkSession, project: dict[str, Any], spies: dict[str, Any]
    ) -> None:
        root, tables = project["root"], project["tables"]
        own = {"lookups": {"dim": _lookup(tables / "weave_ref")}}
        _write(root / "threads" / "t1.thread", _lookup_thread("t1", tables / "out_t1", **own))
        _weave(root, "w1", ["t1.thread"])
        _loom(root, ["w1"])

        result = context.Context(spark, str(root)).run("l.loom")

        assert result.status == "success"
        assert _labels(spark, tables / "out_t1") == ["weave"]
        assert spies["lookups"] == Counter({("thread", "dim"): 1})

    def test_a_failing_weave_still_cleans_it_up(
        self, spark: SparkSession, project: dict[str, Any], spies: dict[str, Any]
    ) -> None:
        root, tables = project["root"], project["tables"]
        _write(root / "threads" / "t1.thread", _lookup_thread("t1", tables / "out_t1"))
        broken = _lookup_thread("t2", tables / "out_t2")
        broken["sources"] = {"ref": {"type": "delta", "alias": str(tables / "missing_src")}}
        _write(root / "threads" / "t2.thread", broken)
        _weave(root, "w1", ["t1.thread"])
        _weave(root, "w2", ["t2.thread"])
        _loom(root, ["w1", "w2"])

        result = context.Context(spark, str(root)).run("l.loom")

        assert result.status != "success"
        assert spies["lookups"] == Counter({("runner", "dim"): 1})
        assert [names for names in spies["cleaned"] if "dim" in names] == [{"dim"}]

    def test_a_filtered_out_threads_lookup_is_not_read(
        self, spark: SparkSession, project: dict[str, Any], spies: dict[str, Any]
    ) -> None:
        root, tables = project["root"], project["tables"]
        _write(root / "threads" / "t1.thread", _lookup_thread("t1", tables / "out_t1"))
        _write(
            root / "threads" / "t2.thread", _rename_thread("t2", tables / "raw", tables / "out_t2")
        )
        _weave(root, "w1", ["t1.thread", "t2.thread"])
        _loom(root, ["w1"])

        result = context.Context(spark, str(root)).run("l.loom", threads=["t2"])

        assert result.status == "success"
        assert spies["lookups"] == Counter()
        assert spies["column_sets"] == Counter({("runner", "cs"): 1})

    def test_used_column_set_resolves_once(
        self, spark: SparkSession, project: dict[str, Any], spies: dict[str, Any]
    ) -> None:
        root, tables = project["root"], project["tables"]
        raw = tables / "raw"
        _write(root / "threads" / "t1.thread", _rename_thread("t1", raw, tables / "out_t1"))
        _write(root / "threads" / "t2.thread", _rename_thread("t2", raw, tables / "out_t2"))
        _weave(root, "w1", ["t1.thread"])
        _weave(root, "w2", ["t2.thread"])
        _loom(root, ["w1", "w2"])

        result = context.Context(spark, str(root)).run("l.loom")

        assert result.status == "success"
        for out in ("out_t1", "out_t2"):
            assert spark.read.format("delta").load(str(tables / out)).columns == ["b"]
        assert spies["column_sets"] == Counter({("runner", "cs"): 1})


class TestStandaloneWeave:
    """A standalone weave materializes the project lookups it uses at weave level."""

    def test_materialized_and_cleaned_by_the_weave(
        self, spark: SparkSession, project: dict[str, Any], spies: dict[str, Any]
    ) -> None:
        root, tables = project["root"], project["tables"]
        _write(root / "threads" / "t1.thread", _lookup_thread("t1", tables / "out_t1"))
        _write(root / "threads" / "t2.thread", _lookup_thread("t2", tables / "out_t2"))
        _weave(root, "w1", ["t1.thread", "t2.thread"])

        result = context.Context(spark, str(root)).run("w1.weave")

        assert result.status == "success"
        assert _labels(spark, tables / "out_t1") == ["project"]
        assert spies["lookups"] == Counter({("runner", "dim"): 1})
        assert [names for names in spies["cleaned"] if "dim" in names] == [{"dim"}]

    def test_unused_registries_are_never_read(
        self, spark: SparkSession, project: dict[str, Any], spies: dict[str, Any]
    ) -> None:
        root, tables = project["root"], project["tables"]
        raw = tables / "raw"
        _write(root / "threads" / "t1.thread", _rename_thread("t1", raw, tables / "out_t1"))
        _weave(root, "w1", ["t1.thread"])

        result = context.Context(spark, str(root)).run("w1.weave")

        assert result.status == "success"
        assert spies["lookups"] == Counter()
        assert spies["column_sets"] == Counter({("runner", "cs"): 1})


class TestStandaloneThread:
    """A thread run on its own resolves the project lookups and column sets it names."""

    def test_lookup_and_column_set(
        self, spark: SparkSession, project: dict[str, Any], spies: dict[str, Any]
    ) -> None:
        root, tables = project["root"], project["tables"]
        _write(root / "threads" / "t1.thread", _lookup_thread("t1", tables / "out_t1"))
        _write(
            root / "threads" / "t2.thread", _rename_thread("t2", tables / "raw", tables / "out_t2")
        )

        ctx = context.Context(spark, str(root))
        assert ctx.run("threads/t1.thread").status == "success"
        assert ctx.run("threads/t2.thread").status == "success"

        assert _labels(spark, tables / "out_t1") == ["project"]
        assert spark.read.format("delta").load(str(tables / "out_t2")).columns == ["b"]
        assert ("thread", "unused") not in spies["lookups"]
        assert spies["column_sets"] == Counter({("context", "cs"): 1})


def test_validate_mode_accepts_project_names(spark: SparkSession, project: dict[str, Any]) -> None:
    root, tables = project["root"], project["tables"]
    _write(root / "threads" / "t1.thread", _lookup_thread("t1", tables / "out_t1"))
    _write(root / "threads" / "t2.thread", _rename_thread("t2", tables / "raw", tables / "out_t2"))
    _weave(root, "w1", ["t1.thread", "t2.thread"])
    _loom(root, ["w1"])

    result = context.Context(spark, str(root)).run("l.loom", mode="validate")

    assert result.status == "success"
