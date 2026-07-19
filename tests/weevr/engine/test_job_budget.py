"""Job-budget regression locks for pipeline step handlers.

Locks the marginal eager-action cost of resolve steps. The baseline
constant documents today's cost; conversions that remove eager actions
ratchet it down and this suite catches any silent reintroduction.

Two instruments, two purposes:

- ``spark_action_counter`` counts Python-side action calls (count,
  collect, ...) — the stable per-step invariant, independent of how
  many JVM jobs each action fans out into.
- ``job_counter`` counts real JVM jobs — used for scan-level
  assertions where work happens without a Python action call (writes,
  observation fulfillment). JVM jobs per action GROW with lineage
  depth here (the O(N^2) recompute this topic removes), so the chain
  lock below is expressed in actions, not jobs.
"""

from typing import Any

import pytest
from pyspark.sql import SparkSession

from weevr.model.pipeline import ResolveParams
from weevr.operations.pipeline.resolve import apply_resolve

# Today each resolve fires two eager fact-scale actions: the duplicate
# count and the stats aggregation. Ratcheted to 0 as the observation
# conversion lands.
EAGER_ACTIONS_PER_RESOLVE = 2


def _lookup(spark: SparkSession):
    return spark.createDataFrame(
        [
            {"code": "A", "sk": 1},
            {"code": "B", "sk": 2},
        ]
    )


def _fact(spark: SparkSession, n: int = 100):
    return spark.range(n).selectExpr("id", "CASE WHEN id % 2 = 0 THEN 'A' ELSE 'B' END AS code")


def _params(name: str) -> ResolveParams:
    return ResolveParams(name=name, lookup="dim", match={"code": "code"}, pk="sk")


@pytest.mark.spark
class TestResolveActionBudget:
    """Marginal eager-action cost of a resolve chain."""

    def test_marginal_actions_per_resolve(self, spark: SparkSession, spark_action_counter) -> None:
        lookup = _lookup(spark)
        df = _fact(spark)

        start = spark_action_counter["total"]
        df = apply_resolve(df, _params("fk_0"), lookup).df
        after_first = spark_action_counter["total"]

        df = apply_resolve(df, _params("fk_1"), lookup).df
        # Final result deliberately unconsumed — the chain exists to count
        # the eager actions its construction fires, never to execute
        apply_resolve(df, _params("fk_2"), lookup)
        after_third = spark_action_counter["total"]

        assert after_first - start == EAGER_ACTIONS_PER_RESOLVE
        marginal = (after_third - after_first) / 2
        assert marginal == EAGER_ACTIONS_PER_RESOLVE


@pytest.mark.spark
class TestResolveActionBudgetObserved:
    """With a registry, the stats aggregation disappears from the chain."""

    # The stats aggregation is observed; the remaining action is the
    # dim-scale lookup pre-check (bounded, permitted). With a reusable
    # unique-key outcome even that disappears — see the zero-action test.
    EAGER_ACTIONS_PER_RESOLVE_OBSERVED = 1

    def test_marginal_actions_with_registry(
        self, spark: SparkSession, spark_action_counter
    ) -> None:
        from weevr.operations.pipeline import ObservationRegistry

        lookup = _lookup(spark)
        registry = ObservationRegistry(scope="main")
        df = _fact(spark)

        start = spark_action_counter["total"]
        df = apply_resolve(df, _params("fk_0"), lookup, observations=registry).df
        apply_resolve(df, _params("fk_1"), lookup, observations=registry)
        after = spark_action_counter["total"]

        assert (after - start) / 2 == self.EAGER_ACTIONS_PER_RESOLVE_OBSERVED

    def test_zero_actions_with_uniqueness_reuse(
        self, spark: SparkSession, spark_action_counter
    ) -> None:
        from weevr.operations.pipeline import LookupMeta, ObservationRegistry

        lookup = _lookup(spark)
        meta = LookupMeta(key_columns=("code",), unique_key_checked=True, unique_key_passed=True)
        registry = ObservationRegistry(scope="main")
        df = _fact(spark)

        start = spark_action_counter["total"]
        df = apply_resolve(df, _params("fk_0"), lookup, observations=registry, lookup_meta=meta).df
        apply_resolve(df, _params("fk_1"), lookup, observations=registry, lookup_meta=meta)
        assert spark_action_counter["total"] - start == 0


@pytest.mark.spark
class TestSingleSourceScan:
    """A multi-resolve chain executes the fact source scan exactly once."""

    def test_chain_construction_runs_zero_jobs_and_scans_once(
        self, spark: SparkSession, job_counter, tmp_path
    ) -> None:
        from weevr.operations.pipeline import LookupMeta, ObservationRegistry

        fact_path = str(tmp_path / "budget_fact")
        _fact(spark, 200).write.format("delta").save(fact_path)
        lookup = _lookup(spark)
        meta = LookupMeta(key_columns=("code",), unique_key_checked=True, unique_key_passed=True)
        registry = ObservationRegistry(scope="main")

        df = spark.read.format("delta").load(fact_path)
        with job_counter() as jc:
            for i in range(3):
                df = apply_resolve(
                    df, _params(f"fk_{i}"), lookup, observations=registry, lookup_meta=meta
                ).df
        # Listener-verified: constructing the chain executes NOTHING — no
        # job may touch the source before the terminal action.
        assert jc.jobs == 0

        # The final plan contains exactly one file-backed relation — the
        # fact source (the lookup is an in-memory frame). Combined with the
        # zero-jobs construction assertion, the single terminal action is
        # the only execution of that scan.
        jdf: Any = df._jdf
        plan = str(jdf.queryExecution().optimizedPlan())
        assert plan.count("parquet") == 1

        with job_counter() as jc_terminal:
            df.collect()
        assert jc_terminal.jobs > 0  # exactly one action executed the chain


@pytest.mark.spark
class TestThreadActionBudget:
    """The headline invariant: a gate-free thread pays the write, only.

    "Fact-scale eager action" here means the Python-side patterns the
    boundary work removed — DataFrame.count()/toPandas() on working
    lineage. Driver-side commit-log reads (history(1).collect()) are the
    design's accepted metadata cost and are not counted.
    """

    @pytest.fixture()
    def eager_spies(self, monkeypatch: pytest.MonkeyPatch) -> dict[str, list[int]]:
        from pyspark.sql import DataFrame

        spies: dict[str, list[int]] = {"count": [], "toPandas": []}
        orig_count = DataFrame.count
        orig_topandas = DataFrame.toPandas

        def _count(inner_self):  # type: ignore[no-untyped-def]
            spies["count"].append(1)
            return orig_count(inner_self)

        def _topandas(inner_self):  # type: ignore[no-untyped-def]
            spies["toPandas"].append(1)
            return orig_topandas(inner_self)

        monkeypatch.setattr(DataFrame, "count", _count)
        monkeypatch.setattr(DataFrame, "toPandas", _topandas)
        return spies

    def test_gate_free_thread_zero_eager_actions(
        self, spark: SparkSession, tmp_delta_path, eager_spies: dict[str, list[int]]
    ) -> None:
        from weevr.engine import execute_thread
        from weevr.model.thread import Thread

        src = tmp_delta_path("budget_gate_src")
        tgt = tmp_delta_path("budget_gate_tgt")
        spark.createDataFrame([{"id": i} for i in range(6)]).write.format("delta").save(src)

        thread = Thread.model_validate(
            {
                "name": "budget_t",
                "config_version": "1",
                "sources": {"main": {"type": "delta", "alias": src}},
                "steps": [{"filter": {"expr": "id < 4"}}],
                "target": {"path": tgt},
                "write": {"mode": "overwrite"},
            }
        )
        result = execute_thread(spark, thread)

        assert result.status == "success", result.error
        # No validations, no warp, no exports, samples off → the write is
        # the only fact-scale execution; telemetry still arrives
        assert eager_spies["count"] == []
        assert eager_spies["toPandas"] == []
        assert result.telemetry is not None
        assert result.telemetry.rows_read == 6
        assert result.telemetry.rows_after_transforms == 4
        assert result.rows_written == 4

    def test_exports_add_no_eager_actions(
        self, spark: SparkSession, tmp_delta_path, eager_spies: dict[str, list[int]]
    ) -> None:
        from weevr.engine import execute_thread
        from weevr.model.thread import Thread

        src = tmp_delta_path("budget_exp_src")
        tgt = tmp_delta_path("budget_exp_tgt")
        exp = tmp_delta_path("budget_exp_out")
        spark.createDataFrame([{"id": i} for i in range(3)]).write.format("delta").save(src)

        thread = Thread.model_validate(
            {
                "name": "budget_exp_t",
                "config_version": "1",
                "sources": {"main": {"type": "delta", "alias": src}},
                "steps": [],
                "target": {"path": tgt},
                "write": {"mode": "overwrite"},
                "exports": [{"name": "e1", "type": "delta", "path": exp}],
            }
        )
        result = execute_thread(spark, thread)
        assert result.status == "success", result.error
        assert eager_spies["count"] == []  # export rides the persisted frame
        assert spark.read.format("delta").load(exp).count() == 3


@pytest.mark.spark
class TestFeatureEnableMatrix:
    """Each observability feature adds only its bounded, enumerated cost."""

    @pytest.fixture()
    def eager_spies(self, monkeypatch: pytest.MonkeyPatch) -> dict[str, list[int]]:
        from pyspark.sql import DataFrame

        spies: dict[str, list[int]] = {"count": [], "toPandas": []}
        orig_count = DataFrame.count
        orig_topandas = DataFrame.toPandas

        def _count(inner_self):  # type: ignore[no-untyped-def]
            spies["count"].append(1)
            return orig_count(inner_self)

        def _topandas(inner_self):  # type: ignore[no-untyped-def]
            spies["toPandas"].append(1)
            return orig_topandas(inner_self)

        monkeypatch.setattr(DataFrame, "count", _count)
        monkeypatch.setattr(DataFrame, "toPandas", _topandas)
        return spies

    @staticmethod
    def _base_cfg(src: str, tgt: str) -> dict:
        return {
            "name": "matrix_t",
            "config_version": "1",
            "sources": {"main": {"type": "delta", "alias": src}},
            "steps": [],
            "target": {"path": tgt},
            "write": {"mode": "overwrite"},
        }

    def test_samples_on_adds_only_sampling_attempts(
        self, spark: SparkSession, tmp_delta_path, eager_spies: dict[str, list[int]]
    ) -> None:
        from weevr.engine import execute_thread
        from weevr.model.thread import Thread

        src = tmp_delta_path("mx_s_src")
        tgt = tmp_delta_path("mx_s_tgt")
        spark.createDataFrame([{"id": 1}]).write.format("delta").save(src)

        execute_thread(spark, Thread.model_validate(self._base_cfg(src, tgt)), capture_samples=True)
        assert eager_spies["count"] == []  # samples add sampling only
        assert 1 <= len(eager_spies["toPandas"]) <= 2  # output (+quarantine) samples

    def test_cte_collector_adds_one_count_per_cte(
        self, spark: SparkSession, tmp_delta_path, eager_spies: dict[str, list[int]]
    ) -> None:
        from weevr.engine import execute_thread
        from weevr.model.thread import Thread
        from weevr.telemetry.collector import SpanCollector

        src = tmp_delta_path("mx_c_src")
        tgt = tmp_delta_path("mx_c_tgt")
        spark.createDataFrame([{"id": 1, "amount": 50}]).write.format("delta").save(src)

        cfg = self._base_cfg(src, tgt)
        cfg["with"] = {"prepped": {"from": "main", "steps": [{"filter": {"expr": "amount > 0"}}]}}
        cfg["steps"] = []
        cfg["sources"] = {"main": {"type": "delta", "alias": src}}

        execute_thread(spark, Thread.model_validate(cfg), collector=SpanCollector(trace_id="t"))
        # Exactly the one collector-gated CTE count; without a collector the
        # gate-free lock elsewhere proves zero
        assert eager_spies["count"] == [1]

    def test_cdc_thread_actions_bounded(
        self, spark: SparkSession, tmp_delta_path, eager_spies: dict[str, list[int]]
    ) -> None:
        from weevr.engine import execute_thread
        from weevr.model.thread import Thread

        src = tmp_delta_path("mx_cdc_src")
        tgt = tmp_delta_path("mx_cdc_tgt")
        spark.createDataFrame(
            [
                {"id": 1, "v": "a", "op": "I", "seq": 1},
                {"id": 2, "v": "b", "op": "I", "seq": 2},
            ]
        ).write.format("delta").save(src)

        cfg = self._base_cfg(src, tgt)
        cfg["name"] = "mx_cdc_t"
        cfg["write"] = {"mode": "merge", "match_keys": ["id"]}
        cfg["load"] = {
            "mode": "cdc",
            "watermark_column": "seq",
            "watermark_type": "long",
            "watermark_store": {"kind": "delta", "path": tgt + "_wm"},
            "cdc": {"operation_column": "op", "insert_value": "I"},
        }
        result = execute_thread(spark, Thread.model_validate(cfg))
        assert result.status == "success", result.error
        # Enumerated, all window-scale against the persisted change window:
        # rows_read + rows_after_transforms (merge-terminal eager pair) +
        # the ordering-column null guard on explicit mappings. Nothing
        # grows with feature count.
        assert len(eager_spies["count"]) == 3
