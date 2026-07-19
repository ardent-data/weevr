"""Explicit cache registry: deterministic consumption, non-fatal misses."""

from typing import Any

import pytest
from pyspark.sql import SparkSession

from weevr.engine.cache_manager import CacheManager
from weevr.engine.planner import build_plan
from weevr.engine.runner import execute_weave
from weevr.model.thread import Thread
from weevr.model.weave import ThreadEntry

pytestmark = pytest.mark.spark


def _thread(name: str, src: str, tgt: str, *, depends: list[str] | None = None) -> Thread:
    cfg: dict[str, Any] = {
        "name": name,
        "config_version": "1",
        "sources": {"main": {"type": "delta", "alias": src}},
        "steps": [],
        "target": {"path": tgt},
        "write": {"mode": "overwrite"},
    }
    if depends:
        cfg["depends_on"] = depends
    return Thread.model_validate(cfg)


class TestRegistryUnit:
    def test_get_returns_registered_snapshot_and_misses_none(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        path = tmp_delta_path("cmreg_tbl")
        spark.createDataFrame([{"id": 1}, {"id": 2}]).write.format("delta").save(path)

        cache = CacheManager(["producer"], {"producer": ["consumer"]})
        cache.persist("producer", spark, path, identity=path.rstrip("/"))

        hit = cache.get(path.rstrip("/"))
        assert hit is not None
        assert hit.count() == 2
        assert cache.get("something/else") is None
        assert cache.get(None) is None
        cache.cleanup()
        assert cache.get(path.rstrip("/")) is None  # unpersist deregisters

    def test_persist_materializes_eagerly(
        self, spark: SparkSession, tmp_delta_path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from pyspark.sql import DataFrame

        path = tmp_delta_path("cmreg_eager")
        spark.createDataFrame([{"id": 1}]).write.format("delta").save(path)

        counts: list[int] = []
        original = DataFrame.count

        def _spy(inner_self):  # type: ignore[no-untyped-def]
            counts.append(1)
            return original(inner_self)

        monkeypatch.setattr(DataFrame, "count", _spy)
        cache = CacheManager(["p"], {"p": ["c"]})
        cache.persist("p", spark, path, identity=path)
        # One eager materialization at persist — consumers never race the
        # cache compute
        assert counts == [1]
        cache.cleanup()


class TestRegistryThroughWeave:
    def test_consumer_served_from_registry_single_storage_read(
        self, spark: SparkSession, tmp_delta_path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Producer writes shared; consumer declares it as a source. The
        # consumer's read must be served from the registry — read_source is
        # never called for that alias.
        import weevr.engine.executor as executor_mod

        src = tmp_delta_path("cmw_src")
        shared = tmp_delta_path("cmw_shared")
        out = tmp_delta_path("cmw_out")
        spark.createDataFrame([{"id": i} for i in range(5)]).write.format("delta").save(src)

        reads: list[str] = []
        original = executor_mod.read_source

        def _spy(spark_arg, alias_arg, source_arg, connections=None):  # type: ignore[no-untyped-def]
            reads.append(source_arg.alias or source_arg.path or "")
            return original(spark_arg, alias_arg, source_arg, connections=connections)

        monkeypatch.setattr(executor_mod, "read_source", _spy)

        out2 = tmp_delta_path("cmw_out2")
        producer = _thread("producer", src, shared)

        def _consumer(name: str, tgt: str) -> Thread:
            return Thread.model_validate(
                {
                    "name": name,
                    "config_version": "1",
                    "sources": {"main": {"type": "delta", "alias": shared}},
                    "steps": [],
                    "target": {"path": tgt},
                    "write": {"mode": "overwrite"},
                    "depends_on": ["producer"],
                }
            )

        threads = {
            "producer": producer,
            "consumer_a": _consumer("consumer_a", out),
            "consumer_b": _consumer("consumer_b", out2),
        }
        plan = build_plan(
            "cmw_weave",
            threads,
            [
                ThreadEntry(name="producer"),
                ThreadEntry(name="consumer_a"),
                ThreadEntry(name="consumer_b"),
            ],
        )
        assert plan.cache_targets == ["producer"]  # >= 2 dependents

        result = execute_weave(spark, plan, threads)
        assert result.status == "success"
        # NEITHER consumer read the shared table from storage — both were
        # served the registered post-write snapshot
        assert all(r != shared for r in reads)
        assert spark.read.format("delta").load(out).count() == 5
        assert spark.read.format("delta").load(out2).count() == 5

    def test_registry_miss_falls_back_to_direct_read(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        # A consumer whose source is NOT any producer's target reads
        # directly and succeeds — a miss is the fallback
        src = tmp_delta_path("cmw_miss_src")
        out = tmp_delta_path("cmw_miss_out")
        spark.createDataFrame([{"id": 1}]).write.format("delta").save(src)

        t = _thread("solo", src, out)
        plan = build_plan("cmw_miss", {"solo": t}, [ThreadEntry(name="solo")])
        result = execute_weave(spark, plan, {"solo": t})
        assert result.status == "success"
        assert spark.read.format("delta").load(out).count() == 1


@pytest.mark.spark
class TestConnectionFormRegistry:
    """Connection-declared cache targets persist AND serve (the latent gap)."""

    def test_connection_target_registers_resolvable_identity(
        self, spark: SparkSession, tmp_delta_path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The runner's registration path resolves a connection-form target
        # to a canonical identity — previously `alias or path` yielded None
        # and the entry was silently unfetchable
        import weevr.engine.runner as runner_mod

        registrations: list[tuple[str, str | None]] = []

        def _persist_spy(self, thread_name, spark_arg, target_path, identity=None):  # type: ignore[no-untyped-def]
            registrations.append((thread_name, identity))
            # Skip the actual read: abfss:// is unreachable in tests — the
            # registration IDENTITY is what this locks

        monkeypatch.setattr(runner_mod.CacheManager, "persist", _persist_spy)

        def _fake_execute_thread(spark_arg, thread, **kwargs):  # type: ignore[no-untyped-def]
            from weevr.engine.result import ThreadResult

            return ThreadResult(
                status="success",
                thread_name=thread.name,
                rows_written=1,
                write_mode="overwrite",
                target_path="unused",
            )

        monkeypatch.setattr(runner_mod, "execute_thread", _fake_execute_thread)

        conn = {"lake": {"type": "onelake", "workspace": "w", "lakehouse": "l"}}
        producer = Thread.model_validate(
            {
                "name": "producer",
                "config_version": "1",
                "sources": {"main": {"type": "delta", "alias": "raw.x"}},
                "steps": [],
                "target": {"connection": "lake", "table": "shared"},
                "connections": conn,
            }
        )

        def _consumer(name: str) -> Thread:
            return Thread.model_validate(
                {
                    "name": name,
                    "config_version": "1",
                    "sources": {"main": {"connection": "lake", "table": "shared"}},
                    "steps": [],
                    "target": {"path": f"/tmp/{name}"},
                    "connections": conn,
                }
            )

        threads = {
            "producer": producer,
            "c1": _consumer("c1"),
            "c2": _consumer("c2"),
        }
        plan = build_plan(
            "conn_weave",
            threads,
            [ThreadEntry(name="producer"), ThreadEntry(name="c1"), ThreadEntry(name="c2")],
        )
        assert plan.cache_targets == ["producer"]  # planner sees connection form

        result = execute_weave(spark, plan, threads)
        assert result.status == "success"
        assert len(registrations) == 1
        name, identity = registrations[0]
        assert name == "producer"
        assert identity is not None and identity.startswith("abfss://")
        assert identity.endswith("shared")

    def test_connection_declared_source_served_from_registry(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        # Consumer side: a connection-declared source whose identity matches
        # a registered entry is served the snapshot (with dedup applied)
        from weevr.engine.cache_manager import CacheManager
        from weevr.engine.executor import _read_source_via_registry
        from weevr.engine.target_identity import resolve_target_identity
        from weevr.model.connection import OneLakeConnection
        from weevr.model.source import Source

        conns = {"lake": OneLakeConnection(type="onelake", workspace="w", lakehouse="l")}
        source = Source.model_validate(
            {
                "connection": "lake",
                "table": "shared",
                "dedup": {"keys": ["id"]},
            }
        )
        identity = resolve_target_identity(
            spark, connection="lake", table="shared", connections=conns
        )
        assert identity is not None

        # Register a snapshot under that identity (backing table is local)
        path = tmp_delta_path("connreg_tbl")
        spark.createDataFrame(
            [{"id": 1, "v": "a"}, {"id": 1, "v": "b"}, {"id": 2, "v": "c"}]
        ).write.format("delta").save(path)
        cache = CacheManager(["producer"], {"producer": ["c"]})
        cache.persist("producer", spark, path, identity=identity)
        try:
            served = _read_source_via_registry(spark, "main", source, conns, cache)
            # Served from the registry — and the source's dedup applied
            assert served.count() == 2
        finally:
            cache.cleanup()
