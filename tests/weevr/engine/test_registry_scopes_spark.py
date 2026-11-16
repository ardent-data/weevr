"""Lookup scope at run time comes from the resolved config tree.

A loom declares lookups ``shared`` and ``loom_only``; weave ``w1`` shadows
``shared``; weave ``w2`` declares none. Each weave has one thread that reads
``shared`` as its only source, so the thread's output says which definition it
saw. Plan, preview and execute must agree, and each declared lookup
materializes once, in the scope that declares it.
"""

from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any

import pytest
from pyspark.sql import SparkSession

from weevr.context import Context

pytestmark = pytest.mark.spark


def _thread(name: str, target: str) -> str:
    return (
        'config_version: "1.0"\n'
        f"name: {name}\n"
        "sources:\n  ref:\n    lookup: shared\n"
        f'target:\n  path: "{target}"\n'
        "write:\n  mode: overwrite\n"
    )


@pytest.fixture
def project(spark: SparkSession, tmp_path: Path) -> dict[str, Any]:
    tables = tmp_path / "tables"
    for name, label in (("loom_ref", "loom"), ("weave_ref", "weave")):
        spark.createDataFrame([(1, label)], ["key", "label"]).write.format("delta").save(
            str(tables / name)
        )
    root = tmp_path / "scopes.weevr"
    (root / "threads").mkdir(parents=True)
    (root / "threads" / "t1.thread").write_text(_thread("t1", str(tables / "out_t1")))
    (root / "threads" / "t2.thread").write_text(_thread("t2", str(tables / "out_t2")))
    (root / "w1.weave").write_text(
        'config_version: "1.0"\n'
        "lookups:\n  shared:\n    source:\n      type: delta\n"
        f'      alias: "{tables / "weave_ref"}"\n    materialize: true\n'
        "threads:\n  - ref: threads/t1.thread\n"
    )
    (root / "w2.weave").write_text('config_version: "1.0"\nthreads:\n  - ref: threads/t2.thread\n')
    (root / "l.loom").write_text(
        'config_version: "1.0"\n'
        "lookups:\n"
        "  shared:\n    source:\n      type: delta\n"
        f'      alias: "{tables / "loom_ref"}"\n    materialize: true\n'
        "  loom_only:\n    source:\n      type: delta\n"
        f'      alias: "{tables / "loom_ref"}"\n    materialize: true\n'
        "weaves:\n  - ref: w1.weave\n  - ref: w2.weave\n"
    )
    return {"root": root, "tables": tables}


def _labels(spark: SparkSession, path: Path) -> list[str]:
    return [row.label for row in spark.read.format("delta").load(str(path)).collect()]


def test_execute_reads_each_threads_effective_lookup(
    spark: SparkSession, project: dict[str, Any]
) -> None:
    result = Context(spark, str(project["root"])).run("l.loom")
    assert result.status == "success"
    assert _labels(spark, project["tables"] / "out_t1") == ["weave"]
    assert _labels(spark, project["tables"] / "out_t2") == ["loom"]


def test_preview_reads_each_threads_effective_lookup(
    spark: SparkSession, project: dict[str, Any]
) -> None:
    result = Context(spark, str(project["root"])).run("l.loom", mode="preview")
    assert result.preview_data is not None
    assert [r.label for r in result.preview_data["t1"].collect()] == ["weave"]
    assert [r.label for r in result.preview_data["t2"].collect()] == ["loom"]


def test_plan_schedules_only_the_weaves_own_lookup(
    spark: SparkSession, project: dict[str, Any]
) -> None:
    result = Context(spark, str(project["root"])).run("l.loom", mode="plan")
    plans = {plan.weave_name: plan for plan in result.execution_plan or []}
    scheduled = {
        name: sorted(n for names in (plan.lookup_schedule or {}).values() for n in names)
        for name, plan in plans.items()
    }
    # w1 materializes its own 'shared'; w2 inherits the loom's, which the loom materializes.
    assert scheduled == {"w1": ["shared"], "w2": []}


def test_each_declared_lookup_materializes_once_in_its_own_scope(
    spark: SparkSession, project: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    import weevr.engine.executor as executor
    import weevr.engine.runner as runner
    from weevr.engine.lookups import materialize_lookups

    materialized: Counter[tuple[str, str]] = Counter()

    def spy(scope: str, real: Any) -> Any:
        def wrapper(spark_: SparkSession, lookups: dict[str, Any], *args: Any, **kwargs: Any):
            for name, lookup in lookups.items():
                if lookup.materialize:
                    materialized[(scope, f"{name}:{lookup.source.alias.rsplit('/', 1)[-1]}")] += 1
            return real(spark_, lookups, *args, **kwargs)

        return wrapper

    monkeypatch.setattr(runner, "materialize_lookups", spy("runner", materialize_lookups))
    monkeypatch.setattr(executor, "materialize_lookups", spy("thread", materialize_lookups))

    result = Context(spark, str(project["root"])).run("l.loom")
    assert result.status == "success"
    # The loom's two lookups once (loom start), w1's own 'shared' once (w1),
    # nothing for w2 or any thread: an inherited lookup is never re-read.
    assert materialized == Counter(
        {
            ("runner", "shared:loom_ref"): 1,
            ("runner", "loom_only:loom_ref"): 1,
            ("runner", "shared:weave_ref"): 1,
        }
    )
