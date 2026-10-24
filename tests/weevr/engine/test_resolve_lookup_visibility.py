"""Which lookups a resolve step and a lookup-bound source can see, end to end.

Each test writes a small project (thread, weave, optionally a loom) over local
Delta tables and runs it through ``Context.run``, so the full runner →
executor → pipeline path is exercised.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from pyspark.sql import SparkSession

from weevr.context import Context
from weevr.engine.result import LoomResult, ThreadResult, WeaveResult
from weevr.errors import ConfigError
from weevr.operations.readers import read_source

pytestmark = pytest.mark.spark

RESOLVE = {"name": "code_sk", "lookup": "dim_codes", "match": "code", "pk": "sk"}


class Project:
    """A throwaway project over local Delta tables."""

    def __init__(self, spark: SparkSession, root: Path) -> None:
        self.spark = spark
        self.root = root
        self.dir = root / "p.weevr"
        self.dir.mkdir()
        self.target = str(root / "tgt")
        self.dim = self.table("dim", [("a", 1), ("b", 2)], ["code", "sk"])
        self.src = self.table("src", [("a",), ("b",)], ["code"])

    def table(self, name: str, rows: list[tuple[Any, ...]], columns: list[str]) -> str:
        path = str(self.root / name)
        self.spark.createDataFrame(rows, columns).write.format("delta").save(path)
        return path

    def lookup(self, table: str, **extra: Any) -> dict[str, Any]:
        return {"source": {"type": "delta", "alias": table}, **extra}

    def write(self, name: str, body: dict[str, Any]) -> None:
        (self.dir / name).write_text(json.dumps({"config_version": "1.0", **body}))

    def thread(self, src: str, steps: list[dict[str, Any]], **extra: Any) -> None:
        sources = {"main": {"type": "delta", "alias": src}, **extra.pop("sources", {})}
        self.write(
            "fact.thread",
            {
                "sources": sources,
                "steps": steps,
                "target": {"path": self.target},
                "write": {"mode": "overwrite"},
                **extra,
            },
        )

    def run(self, config: str) -> tuple[str, list[str]]:
        result = Context(spark=self.spark, project=str(self.dir)).run(config)
        detail = result.detail
        if isinstance(detail, LoomResult):
            threads = [t for weave in detail.weave_results for t in weave.thread_results]
        elif isinstance(detail, WeaveResult):
            threads = list(detail.thread_results)
        elif isinstance(detail, ThreadResult):
            threads = [detail]
        else:
            raise TypeError(f"unexpected run detail: {type(detail).__name__}")
        assert threads, "the run produced no thread results"
        return result.status, [t.error for t in threads if t.error]

    def resolved(self) -> dict[str, Any]:
        rows = self.spark.read.format("delta").load(self.target).collect()
        return {r["code"]: r["code_sk"] for r in rows}


@pytest.fixture
def project(spark: SparkSession, tmp_path: Path) -> Project:
    return Project(spark, tmp_path)


def _weave(project: Project, lookups: dict[str, Any] | None = None) -> None:
    body: dict[str, Any] = {"threads": [{"ref": "fact.thread"}]}
    if lookups:
        body["lookups"] = lookups
    project.write("w.weave", body)


class TestByNameThreadAndWeave:
    """A resolve step finds a thread or weave lookup by name, no source binding."""

    @pytest.mark.parametrize("materialize", [False, True])
    def test_weave_lookup_by_name(self, project: Project, materialize: bool) -> None:
        project.thread(project.src, [{"resolve": RESOLVE}])
        _weave(project, {"dim_codes": project.lookup(project.dim, materialize=materialize)})
        assert project.run("w.weave") == ("success", [])
        assert project.resolved() == {"a": 1, "b": 2}

    def test_thread_lookup_by_name(self, project: Project) -> None:
        project.thread(
            project.src,
            [{"resolve": RESOLVE}],
            lookups={"dim_codes": project.lookup(project.dim)},
        )
        _weave(project)
        assert project.run("w.weave") == ("success", [])
        assert project.resolved() == {"a": 1, "b": 2}

    def test_standalone_thread_lookup_by_name(self, project: Project) -> None:
        project.thread(
            project.src,
            [{"resolve": RESOLVE}],
            lookups={"dim_codes": project.lookup(project.dim)},
        )
        assert project.run("fact.thread") == ("success", [])
        assert project.resolved() == {"a": 1, "b": 2}

    def test_batch_items_by_name(self, project: Project) -> None:
        batch = {"batch": [{"name": "code_sk", "lookup": "dim_codes", "match": "code", "pk": "sk"}]}
        project.thread(project.src, [{"resolve": batch}])
        _weave(project, {"dim_codes": project.lookup(project.dim)})
        assert project.run("w.weave") == ("success", [])
        assert project.resolved() == {"a": 1, "b": 2}

    def test_with_block_resolve_by_name(self, project: Project) -> None:
        project.thread(
            project.src,
            [{"join": {"source": "keyed", "type": "left", "on": ["code"]}}],
            **{"with": {"keyed": {"from": "main", "steps": [{"resolve": RESOLVE}]}}},
        )
        _weave(project, {"dim_codes": project.lookup(project.dim)})
        assert project.run("w.weave") == ("success", [])
        assert project.resolved() == {"a": 1, "b": 2}


class TestBoundSources:
    """Lookup-bound sources keep working and win on a name clash."""

    def test_bound_alias_with_a_different_name(self, project: Project) -> None:
        project.thread(
            project.src,
            [{"resolve": {**RESOLVE, "lookup": "fk_dim"}}],
            sources={"fk_dim": {"lookup": "dim_codes"}},
        )
        _weave(project, {"dim_codes": project.lookup(project.dim)})
        assert project.run("w.weave") == ("success", [])
        assert project.resolved() == {"a": 1, "b": 2}

    def test_bound_alias_wins_over_definition_name(self, project: Project) -> None:
        other = project.table("other", [("a", 10), ("b", 20)], ["code", "sk"])
        project.thread(
            project.src,
            [{"resolve": RESOLVE}],
            sources={"dim_codes": {"lookup": "other_dim"}},
        )
        _weave(
            project,
            {
                "dim_codes": project.lookup(project.dim),
                "other_dim": project.lookup(other),
            },
        )
        assert project.run("w.weave") == ("success", [])
        assert project.resolved() == {"a": 10, "b": 20}

    def test_definition_meta_not_applied_to_shadowing_alias(self, project: Project) -> None:
        """The alias frame has duplicate keys; the definition's unique-key pass must not hide them.

        A shadowing alias must not inherit the definition's materialization facts.
        """
        dupes = project.table("dupes", [("a", 10), ("a", 11), ("b", 20)], ["code", "sk"])
        project.thread(
            project.src,
            [{"resolve": {**RESOLVE, "on_duplicate": "error"}}],
            sources={"dim_codes": {"lookup": "dupe_dim"}},
        )
        _weave(
            project,
            {
                "dim_codes": project.lookup(
                    project.dim,
                    materialize=True,
                    key=["code"],
                    unique_key=True,
                ),
                "dupe_dim": project.lookup(dupes),
            },
        )
        status, errors = project.run("w.weave")
        assert status == "failure"
        assert any("duplicate" in e.lower() for e in errors)


class TestReadOnce:
    """A lookup used by a bound source and by name is read once per thread."""

    def test_non_materialized_lookup_read_once(
        self, project: Project, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        reads: list[str] = []
        real_read = read_source

        def counting_read(spark: Any, name: str, *args: Any, **kwargs: Any) -> Any:
            reads.append(name)
            return real_read(spark, name, *args, **kwargs)

        monkeypatch.setattr("weevr.engine.lookups.read_source", counting_read)
        project.thread(
            project.src,
            [
                {"resolve": {**RESOLVE, "lookup": "fk_dim"}},
                {"resolve": {**RESOLVE, "name": "code_sk2"}},
            ],
            sources={"fk_dim": {"lookup": "dim_codes"}},
        )
        _weave(project, {"dim_codes": project.lookup(project.dim)})
        assert project.run("w.weave") == ("success", [])
        assert reads.count("dim_codes") == 1


class TestUndefined:
    """A name visible nowhere: abort fails the load, warn runs with on_unknown."""

    def test_abort_fails_the_load(self, project: Project) -> None:
        project.thread(project.src, [{"resolve": {**RESOLVE, "lookup": "nope"}}])
        _weave(project, {"dim_codes": project.lookup(project.dim)})
        with pytest.raises(ConfigError, match="undefined lookup 'nope'"):
            project.run("w.weave")

    def test_warn_assigns_unknown(self, project: Project) -> None:
        step = {**RESOLVE, "lookup": "nope", "on_failure": "warn"}
        project.thread(project.src, [{"resolve": step}])
        _weave(project)
        assert project.run("w.weave") == ("success", [])
        assert project.resolved() == {"a": -1, "b": -1}


class TestProducerOrdering:
    """A by-name resolve consumer runs after the thread that writes the lookup's table."""

    def test_consumer_waits_for_producer(self, project: Project) -> None:
        produced = str(project.root / "produced_dim")
        project.write(
            "producer.thread",
            {
                "sources": {"raw": {"type": "delta", "alias": project.dim}},
                "target": {"path": produced},
                "write": {"mode": "overwrite"},
            },
        )
        project.thread(project.src, [{"resolve": RESOLVE}])
        project.write(
            "w.weave",
            {
                "lookups": {"dim_codes": project.lookup(produced)},
                "threads": [{"ref": "fact.thread"}, {"ref": "producer.thread"}],
            },
        )
        assert project.run("w.weave") == ("success", [])
        assert project.resolved() == {"a": 1, "b": 2}


def _loom(project: Project, lookups: dict[str, Any], weaves: list[str]) -> None:
    project.write("l.loom", {"lookups": lookups, "weaves": [{"ref": w} for w in weaves]})


class TestLoomLookups:
    """Threads can use loom lookups, bound or by name; narrower scopes win."""

    @pytest.mark.parametrize("materialize", [False, True])
    def test_bound_source_to_loom_lookup(self, project: Project, materialize: bool) -> None:
        project.thread(
            project.src,
            [{"resolve": {**RESOLVE, "lookup": "fk_dim"}}],
            sources={"fk_dim": {"lookup": "dim_codes"}},
        )
        _weave(project)
        _loom(
            project,
            {"dim_codes": project.lookup(project.dim, materialize=materialize)},
            ["w.weave"],
        )
        assert project.run("l.loom") == ("success", [])
        assert project.resolved() == {"a": 1, "b": 2}

    @pytest.mark.parametrize("materialize", [False, True])
    def test_loom_lookup_by_name(self, project: Project, materialize: bool) -> None:
        project.thread(project.src, [{"resolve": RESOLVE}])
        _weave(project)
        _loom(
            project,
            {"dim_codes": project.lookup(project.dim, materialize=materialize)},
            ["w.weave"],
        )
        assert project.run("l.loom") == ("success", [])
        assert project.resolved() == {"a": 1, "b": 2}

    def test_materialized_loom_lookup_read_once(
        self, project: Project, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        reads: list[str] = []

        def counting_read(spark: Any, name: str, *args: Any, **kwargs: Any) -> Any:
            reads.append(name)
            return read_source(spark, name, *args, **kwargs)

        monkeypatch.setattr("weevr.engine.lookups.read_source", counting_read)
        project.thread(project.src, [{"resolve": RESOLVE}])
        _weave(project)
        project.write("w2.weave", {"threads": [{"ref": "fact.thread"}]})
        _loom(
            project,
            {"dim_codes": project.lookup(project.dim, materialize=True)},
            ["w.weave", "w2.weave"],
        )
        assert project.run("l.loom") == ("success", [])
        assert reads.count("dim_codes") == 1

    def test_weave_lookup_shadows_loom_lookup(self, project: Project) -> None:
        weave_dim = project.table("weave_dim", [("a", 10), ("b", 20)], ["code", "sk"])
        project.thread(project.src, [{"resolve": RESOLVE}])
        _weave(project, {"dim_codes": project.lookup(weave_dim)})
        _loom(project, {"dim_codes": project.lookup(project.dim, materialize=True)}, ["w.weave"])
        assert project.run("l.loom") == ("success", [])
        assert project.resolved() == {"a": 10, "b": 20}

    def test_thread_lookup_shadows_weave_and_loom(self, project: Project) -> None:
        weave_dim = project.table("weave_dim", [("a", 10), ("b", 20)], ["code", "sk"])
        thread_dim = project.table("thread_dim", [("a", 100), ("b", 200)], ["code", "sk"])
        project.thread(
            project.src,
            [{"resolve": RESOLVE}],
            lookups={"dim_codes": project.lookup(thread_dim)},
        )
        _weave(project, {"dim_codes": project.lookup(weave_dim, materialize=True)})
        _loom(project, {"dim_codes": project.lookup(project.dim, materialize=True)}, ["w.weave"])
        assert project.run("l.loom") == ("success", [])
        assert project.resolved() == {"a": 100, "b": 200}


class TestShadowedLookupFacts:
    """A narrower lookup doesn't inherit a wider lookup's unique-key result."""

    @staticmethod
    def _unique(project: Project) -> dict[str, Any]:
        return project.lookup(project.dim, materialize=True, key=["code"], unique_key=True)

    @staticmethod
    def _dupes(project: Project) -> dict[str, Any]:
        table = project.table("dupes", [("a", 10), ("a", 11), ("b", 20)], ["code", "sk"])
        return project.lookup(table)

    def test_weave_lookup_over_loom_lookup(self, project: Project) -> None:
        project.thread(project.src, [{"resolve": {**RESOLVE, "on_duplicate": "error"}}])
        _weave(project, {"dim_codes": self._dupes(project)})
        _loom(project, {"dim_codes": self._unique(project)}, ["w.weave"])
        status, errors = project.run("l.loom")
        assert status == "failure"
        assert any("duplicate" in e.lower() for e in errors)

    def test_thread_lookup_over_weave_lookup(self, project: Project) -> None:
        project.thread(
            project.src,
            [{"resolve": {**RESOLVE, "on_duplicate": "error"}}],
            lookups={"dim_codes": self._dupes(project)},
        )
        _weave(project, {"dim_codes": self._unique(project)})
        status, errors = project.run("w.weave")
        assert status == "failure"
        assert any("duplicate" in e.lower() for e in errors)
