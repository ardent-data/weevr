"""End-to-end lookup-meta plumbing: scoped unique-key reuse reaches resolve.

Locks the four-site materialization-capture plumbing: a lookup whose
unique-key check passed at materialization must skip the resolve-time
duplicate pre-check when consumed through the real engine paths. A
missed capture site degrades silently to the fresh pre-check (safe but
wasteful) — invisible to outcome assertions, so these tests spy on the
pre-check itself.
"""

from typing import Any

import pytest
from pyspark.sql import SparkSession

from weevr.engine import execute_thread
from weevr.engine.planner import build_plan
from weevr.engine.runner import execute_weave
from weevr.model.lookup import Lookup
from weevr.model.thread import Thread
from weevr.model.weave import ThreadEntry

pytestmark = pytest.mark.spark


@pytest.fixture()
def precheck_spy(monkeypatch: pytest.MonkeyPatch) -> list[Any]:
    """Record every invocation of the resolve duplicate pre-check."""
    from weevr.operations.pipeline import resolve as resolve_mod

    calls: list[Any] = []
    original = resolve_mod._has_duplicate_join_keys

    def _spy(lookup_df, join_columns):  # type: ignore[no-untyped-def]
        calls.append(list(join_columns))
        return original(lookup_df, join_columns)

    monkeypatch.setattr(resolve_mod, "_has_duplicate_join_keys", _spy)
    return calls


def _write_dim(spark: SparkSession, path: str) -> None:
    spark.createDataFrame([{"sk": 1, "code": "A"}, {"sk": 2, "code": "B"}]).write.format(
        "delta"
    ).save(path)


def _write_src(spark: SparkSession, path: str) -> None:
    spark.createDataFrame(
        [{"id": 1, "code": "A"}, {"id": 2, "code": "B"}, {"id": 3, "code": "A"}]
    ).write.format("delta").save(path)


def _lookup_cfg(dim_path: str, *, unique_key: bool) -> dict[str, Any]:
    return {
        "source": {"type": "delta", "alias": dim_path},
        "materialize": True,
        "key": ["code"],
        "unique_key": unique_key,
    }


def _thread_cfg(src_path: str, tgt_path: str, *, lookups: dict[str, Any] | None = None) -> Thread:
    cfg: dict[str, Any] = {
        "name": "fact_t",
        "config_version": "1",
        "sources": {
            "main": {"type": "delta", "alias": src_path},
            "dim_codes": {"lookup": "dim_codes"},
        },
        "steps": [
            {
                "resolve": {
                    "name": "code_sk",
                    "lookup": "dim_codes",
                    "match": {"code": "code"},
                    "pk": "sk",
                }
            }
        ],
        "target": {"path": tgt_path},
        "write": {"mode": "overwrite"},
    }
    if lookups is not None:
        cfg["lookups"] = lookups
    return Thread.model_validate(cfg)


class TestThreadScopedReuse:
    def test_thread_lookup_unique_key_skips_precheck(
        self, spark: SparkSession, tmp_delta_path, precheck_spy: list[Any]
    ) -> None:
        dim = tmp_delta_path("meta_dim_t1")
        src = tmp_delta_path("meta_src_t1")
        tgt = tmp_delta_path("meta_tgt_t1")
        _write_dim(spark, dim)
        _write_src(spark, src)

        thread = _thread_cfg(src, tgt, lookups={"dim_codes": _lookup_cfg(dim, unique_key=True)})
        result = execute_thread(spark, thread)

        assert result.status == "success", result.error
        assert precheck_spy == []  # materialization outcome reused end-to-end

    def test_thread_lookup_without_check_runs_precheck(
        self, spark: SparkSession, tmp_delta_path, precheck_spy: list[Any]
    ) -> None:
        dim = tmp_delta_path("meta_dim_t2")
        src = tmp_delta_path("meta_src_t2")
        tgt = tmp_delta_path("meta_tgt_t2")
        _write_dim(spark, dim)
        _write_src(spark, src)

        thread = _thread_cfg(src, tgt, lookups={"dim_codes": _lookup_cfg(dim, unique_key=False)})
        result = execute_thread(spark, thread)

        assert result.status == "success", result.error
        assert precheck_spy == [["code"]]  # no reusable outcome → fresh pre-check


class TestWeaveScopedReuse:
    def test_weave_lookup_unique_key_skips_precheck(
        self, spark: SparkSession, tmp_delta_path, precheck_spy: list[Any]
    ) -> None:
        dim = tmp_delta_path("meta_dim_w1")
        src = tmp_delta_path("meta_src_w1")
        tgt = tmp_delta_path("meta_tgt_w1")
        _write_dim(spark, dim)
        _write_src(spark, src)

        thread = _thread_cfg(src, tgt)  # lookup declared at weave scope
        threads = {"fact_t": thread}
        plan = build_plan("meta_weave", threads, [ThreadEntry(name="fact_t")])
        lookups = {"dim_codes": Lookup.model_validate(_lookup_cfg(dim, unique_key=True))}

        result = execute_weave(spark, plan, threads, lookups=lookups)

        assert result.status == "success"
        assert precheck_spy == []
