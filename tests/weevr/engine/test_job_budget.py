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
        df = apply_resolve(df, _params("fk_2"), lookup).df
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
        df = apply_resolve(df, _params("fk_1"), lookup, observations=registry).df
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
        df = apply_resolve(df, _params("fk_1"), lookup, observations=registry, lookup_meta=meta).df
        assert spark_action_counter["total"] - start == 0
