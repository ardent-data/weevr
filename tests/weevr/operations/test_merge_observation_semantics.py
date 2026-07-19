"""Empirical locks for merge-time observation and metric-key semantics.

Two load-bearing assumptions of the single-execution telemetry design,
verified against the pinned pyspark/delta-spark line rather than assumed:

(a) an ``Observation`` attached to a MERGE's *source* plan — Delta's
    classic MERGE INTO may execute the source more than once internally
    (touched-files pass + write pass), and ``observe`` semantics are
    defined for a single execution;
(b) the Delta commit ``operationMetrics`` expose the *split* matched
    update keys (``numTargetRowsMatchedUpdated`` /
    ``numTargetRowsNotMatchedBySourceUpdated``) rather than only the
    conflated ``numTargetRowsUpdated`` aggregate.

If either lock breaks on a runtime upgrade, the recorded fallbacks apply:
CDC counts move to one bounded action on the persisted change window, and
the dimension split degrades to absent-with-warning for the
track_history + soft_delete combination.
"""

import pytest
from delta.tables import DeltaTable
from pyspark.sql import Observation, SparkSession
from pyspark.sql import functions as F

pytestmark = pytest.mark.spark


def _seed_target(spark: SparkSession, path: str) -> DeltaTable:
    spark.createDataFrame(
        [{"id": 1, "v": "a"}, {"id": 2, "v": "b"}, {"id": 3, "v": "c"}]
    ).write.format("delta").save(path)
    return DeltaTable.forPath(spark, path)


class TestMergeSourceObservation:
    def test_merge_fulfills_source_observation_with_zero_rows(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        path = tmp_delta_path("obs_merge_tgt")
        target = _seed_target(spark, path)

        # Source: one update (id=2), one insert (id=4), one absent (id=1,3)
        obs = Observation("merge_src_probe")
        source = spark.createDataFrame([{"id": 2, "v": "B"}, {"id": 4, "v": "d"}]).observe(
            obs, F.count(F.lit(1)).alias("n"), F.sum("id").alias("id_sum")
        )

        (
            target.alias("t")
            .merge(source.alias("s"), "t.id = s.id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .whenNotMatchedBySourceUpdate(set={"v": F.lit("gone")})
            .execute()
        )

        row = dict(obs.get)
        # VERDICT (2026-07-19, pyspark 3.5 / delta pinned): the merge
        # fulfills the observation WITHOUT flowing source rows through the
        # observed node — it fires with zero rows. Merge-fulfilled
        # observations are therefore UNUSABLE for source-side counts; the
        # engine uses bounded window-scale actions instead (the recorded
        # fallback). This lock pins the misbehavior: if a runtime upgrade
        # makes this exactly-once (n=2, id_sum=6), the assertion fails and
        # the observation mechanism can be reconsidered.
        assert row == {"n": 0, "id_sum": None}

    def test_split_update_metric_keys_present(self, spark: SparkSession, tmp_delta_path) -> None:
        path = tmp_delta_path("obs_merge_metrics")
        target = _seed_target(spark, path)

        source = spark.createDataFrame([{"id": 2, "v": "B"}, {"id": 4, "v": "d"}])
        (
            target.alias("t")
            .merge(source.alias("s"), "t.id = s.id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .whenNotMatchedBySourceUpdate(set={"v": F.lit("gone")})
            .execute()
        )

        metrics = (
            DeltaTable.forPath(spark, path).history(1).select("operationMetrics").collect()[0][0]
        )
        # The split keys the dimension-metrics arithmetic depends on
        assert "numTargetRowsMatchedUpdated" in metrics
        assert "numTargetRowsNotMatchedBySourceUpdated" in metrics
        assert int(metrics["numTargetRowsMatchedUpdated"]) == 1  # id=2
        assert int(metrics["numTargetRowsNotMatchedBySourceUpdated"]) == 2  # ids 1,3
        assert int(metrics["numTargetRowsInserted"]) == 1  # id=4
        # And the aggregate conflates them — the reason it must never be
        # used for the split
        assert int(metrics["numTargetRowsUpdated"]) == 3
