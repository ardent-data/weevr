"""Tests for resolve step handler — FK resolution via lookup join."""

from typing import Any

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from weevr.errors.exceptions import ExecutionError
from weevr.model.pipeline import ResolveParams, Step
from weevr.operations.pipeline.resolve import apply_resolve

pytestmark = pytest.mark.spark


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _fact_schema(*cols: str) -> StructType:
    """Build a fact StructType with string source columns."""
    return StructType([StructField(c, StringType(), True) for c in cols])


def _dim_df(spark: SparkSession):
    """Simple dimension: (id INT, natural_id STRING)."""
    return spark.createDataFrame(
        [(1, "A"), (2, "B"), (3, "C")],
        StructType(
            [
                StructField("id", IntegerType(), False),
                StructField("natural_id", StringType(), False),
            ]
        ),
    )


# ---------------------------------------------------------------------------
# Single FK resolution
# ---------------------------------------------------------------------------


class TestApplyResolveSingleFK:
    """Test single FK resolution scenarios."""

    def test_basic_match(self, spark: SparkSession):
        """Matching BKs resolve to the correct surrogate key."""
        fact = spark.createDataFrame([("A",), ("B",), ("C",)], _fact_schema("bk"))
        dim = _dim_df(spark)
        params = ResolveParams(
            name="plant_id",
            lookup="dim_plant",
            match={"bk": "natural_id"},
            pk="id",
        )
        result = apply_resolve(fact, params, dim)
        rows = {r["bk"]: r["plant_id"] for r in result.df.collect()}
        assert rows == {"A": 1, "B": 2, "C": 3}

    def test_unknown_bk(self, spark: SparkSession):
        """BK not found in dim gets on_unknown sentinel."""
        fact = spark.createDataFrame([("A",), ("Z",)], _fact_schema("bk"))
        dim = _dim_df(spark)
        params = ResolveParams(
            name="plant_id",
            lookup="dim_plant",
            match={"bk": "natural_id"},
            pk="id",
            on_unknown=-1,
        )
        result = apply_resolve(fact, params, dim)
        rows = {r["bk"]: r["plant_id"] for r in result.df.collect()}
        assert rows["A"] == 1
        assert rows["Z"] == -1

    def test_invalid_bk_null(self, spark: SparkSession):
        """Null BK gets on_invalid sentinel."""
        fact = spark.createDataFrame([("A",), (None,)], _fact_schema("bk"))
        dim = _dim_df(spark)
        params = ResolveParams(
            name="plant_id",
            lookup="dim_plant",
            match={"bk": "natural_id"},
            pk="id",
            on_invalid=-4,
        )
        result = apply_resolve(fact, params, dim)
        rows = result.df.collect()
        valid = [r for r in rows if r["bk"] == "A"]
        invalid = [r for r in rows if r["bk"] is None]
        assert valid[0]["plant_id"] == 1
        assert invalid[0]["plant_id"] == -4

    def test_invalid_bk_blank(self, spark: SparkSession):
        """Blank/whitespace BK gets on_invalid sentinel."""
        fact = spark.createDataFrame([("A",), ("  ",)], _fact_schema("bk"))
        dim = _dim_df(spark)
        params = ResolveParams(
            name="plant_id",
            lookup="dim_plant",
            match={"bk": "natural_id"},
            pk="id",
            on_invalid=-4,
        )
        result = apply_resolve(fact, params, dim)
        rows = result.df.collect()
        blank = [r for r in rows if r["bk"] is not None and r["bk"].strip() == ""]
        assert blank[0]["plant_id"] == -4

    def test_compound_bk(self, spark: SparkSession):
        """Multi-column match dict resolves compound BK."""
        fact = spark.createDataFrame(
            [("X", "1"), ("Y", "2")],
            StructType(
                [
                    StructField("region", StringType(), True),
                    StructField("plant", StringType(), True),
                ]
            ),
        )
        dim = spark.createDataFrame(
            [(10, "X", "1"), (20, "Y", "2")],
            StructType(
                [
                    StructField("id", IntegerType(), False),
                    StructField("region_code", StringType(), False),
                    StructField("plant_code", StringType(), False),
                ]
            ),
        )
        params = ResolveParams(
            name="plant_id",
            lookup="dim_plant",
            match={"region": "region_code", "plant": "plant_code"},
            pk="id",
        )
        result = apply_resolve(fact, params, dim)
        rows = {r["region"]: r["plant_id"] for r in result.df.collect()}
        assert rows == {"X": 10, "Y": 20}

    def test_include_columns_list(self, spark: SparkSession):
        """Include adds extra columns from lookup by name."""
        fact = spark.createDataFrame([("A",)], _fact_schema("bk"))
        dim = spark.createDataFrame(
            [(1, "A", "Plant Alpha")],
            StructType(
                [
                    StructField("id", IntegerType(), False),
                    StructField("natural_id", StringType(), False),
                    StructField("description", StringType(), True),
                ]
            ),
        )
        params = ResolveParams(
            name="plant_id",
            lookup="dim_plant",
            match={"bk": "natural_id"},
            pk="id",
            include=["description"],
        )
        result = apply_resolve(fact, params, dim)
        row = result.df.collect()[0]
        assert row["plant_id"] == 1
        assert row["description"] == "Plant Alpha"

    def test_include_columns_dict_rename(self, spark: SparkSession):
        """Include with dict renames columns from lookup."""
        fact = spark.createDataFrame([("A",)], _fact_schema("bk"))
        dim = spark.createDataFrame(
            [(1, "A", "Plant Alpha")],
            StructType(
                [
                    StructField("id", IntegerType(), False),
                    StructField("natural_id", StringType(), False),
                    StructField("description", StringType(), True),
                ]
            ),
        )
        params = ResolveParams(
            name="plant_id",
            lookup="dim_plant",
            match={"bk": "natural_id"},
            pk="id",
            include={"description": "plant_desc"},
        )
        result = apply_resolve(fact, params, dim)
        row = result.df.collect()[0]
        assert "plant_desc" in result.df.columns
        assert row["plant_desc"] == "Plant Alpha"

    def test_include_prefix(self, spark: SparkSession):
        """Include prefix prepends to included column names."""
        fact = spark.createDataFrame([("A",)], _fact_schema("bk"))
        dim = spark.createDataFrame(
            [(1, "A", "Plant Alpha")],
            StructType(
                [
                    StructField("id", IntegerType(), False),
                    StructField("natural_id", StringType(), False),
                    StructField("description", StringType(), True),
                ]
            ),
        )
        params = ResolveParams(
            name="plant_id",
            lookup="dim_plant",
            match={"bk": "natural_id"},
            pk="id",
            include=["description"],
            include_prefix="dim_",
        )
        result = apply_resolve(fact, params, dim)
        assert "dim_description" in result.df.columns

    def test_drop_source_columns(self, spark: SparkSession):
        """drop_source_columns removes source columns after resolve."""
        fact = spark.createDataFrame([("A",)], _fact_schema("bk"))
        dim = _dim_df(spark)
        params = ResolveParams(
            name="plant_id",
            lookup="dim_plant",
            match={"bk": "natural_id"},
            pk="id",
            drop_source_columns=True,
        )
        result = apply_resolve(fact, params, dim)
        assert "bk" not in result.df.columns
        assert "plant_id" in result.df.columns

    def test_normalize_trim_lower(self, spark: SparkSession):
        """Normalization with trim_lower matches regardless of case/whitespace."""
        fact = spark.createDataFrame([(" a ",)], _fact_schema("bk"))
        dim = spark.createDataFrame(
            [(1, "A")],
            StructType(
                [
                    StructField("id", IntegerType(), False),
                    StructField("natural_id", StringType(), False),
                ]
            ),
        )
        params = ResolveParams(
            name="plant_id",
            lookup="dim_plant",
            match={"bk": "natural_id"},
            pk="id",
            normalize="trim_lower",
        )
        result = apply_resolve(fact, params, dim)
        row = result.df.collect()[0]
        assert row["plant_id"] == 1

    def test_on_duplicate_first(self, spark: SparkSession):
        """on_duplicate=first takes first match silently."""
        fact = spark.createDataFrame([("A",)], _fact_schema("bk"))
        dim = spark.createDataFrame(
            [(1, "A"), (2, "A")],
            StructType(
                [
                    StructField("id", IntegerType(), False),
                    StructField("natural_id", StringType(), False),
                ]
            ),
        )
        params = ResolveParams(
            name="plant_id",
            lookup="dim_plant",
            match={"bk": "natural_id"},
            pk="id",
            on_duplicate="first",
        )
        result = apply_resolve(fact, params, dim)
        rows = result.df.collect()
        assert len(rows) == 1
        assert rows[0]["plant_id"] in (1, 2)

    def test_on_duplicate_error(self, spark: SparkSession):
        """on_duplicate=error raises on multiple matches."""
        fact = spark.createDataFrame([("A",)], _fact_schema("bk"))
        dim = spark.createDataFrame(
            [(1, "A"), (2, "A")],
            StructType(
                [
                    StructField("id", IntegerType(), False),
                    StructField("natural_id", StringType(), False),
                ]
            ),
        )
        params = ResolveParams(
            name="plant_id",
            lookup="dim_plant",
            match={"bk": "natural_id"},
            pk="id",
            on_duplicate="error",
        )
        with pytest.raises(Exception, match="[Dd]uplicate"):
            apply_resolve(fact, params, dim)

    def test_metadata_stats(self, spark: SparkSession):
        """Result metadata contains resolution stats."""
        fact = spark.createDataFrame([("A",), ("Z",), (None,)], _fact_schema("bk"))
        dim = _dim_df(spark)
        params = ResolveParams(
            name="plant_id",
            lookup="dim_plant",
            match={"bk": "natural_id"},
            pk="id",
        )
        result = apply_resolve(fact, params, dim)
        stats = result.metadata.get("resolve_stats", {}).get("plant_id")
        assert stats is not None
        assert stats["total"] == 3
        assert stats["matched"] == 1
        assert stats["unknown"] == 1
        assert stats["invalid"] == 1

    def test_row_count_preserved(self, spark: SparkSession):
        """Output row count equals input row count."""
        fact = spark.createDataFrame([("A",), ("B",), ("Z",), (None,)], _fact_schema("bk"))
        dim = _dim_df(spark)
        params = ResolveParams(
            name="plant_id",
            lookup="dim_plant",
            match={"bk": "natural_id"},
            pk="id",
        )
        result = apply_resolve(fact, params, dim)
        assert result.df.count() == 4

    def test_lookup_bk_columns_dropped(self, spark: SparkSession):
        """Lookup BK columns are not present in result."""
        fact = spark.createDataFrame([("A",)], _fact_schema("bk"))
        dim = _dim_df(spark)
        params = ResolveParams(
            name="plant_id",
            lookup="dim_plant",
            match={"bk": "natural_id"},
            pk="id",
        )
        result = apply_resolve(fact, params, dim)
        assert "natural_id" not in result.df.columns


# ---------------------------------------------------------------------------
# Effective block and where predicate (Task 6)
# ---------------------------------------------------------------------------


class TestApplyResolveEffective:
    """Test effective block and where predicate filtering."""

    def test_current_flag_boolean(self, spark: SparkSession):
        """Current flag with boolean true filters to active records."""
        fact = spark.createDataFrame([("A",)], _fact_schema("bk"))
        dim = spark.createDataFrame(
            [(1, "A", True), (2, "A", False)],
            ["id", "natural_id", "is_current"],
        )
        params = ResolveParams(
            name="fk",
            lookup="dim",
            match={"bk": "natural_id"},
            pk="id",
            effective={"current": "is_current"},  # type: ignore[arg-type]
            on_duplicate="first",
        )
        result = apply_resolve(fact, params, dim)
        row = result.df.collect()[0]
        assert row["fk"] == 1

    def test_current_flag_custom_value(self, spark: SparkSession):
        """Current flag with custom value 'Y' filters correctly."""
        fact = spark.createDataFrame([("A",)], _fact_schema("bk"))
        dim = spark.createDataFrame(
            [(1, "A", "Y"), (2, "A", "N")],
            StructType(
                [
                    StructField("id", IntegerType(), False),
                    StructField("natural_id", StringType(), False),
                    StructField("is_current", StringType(), True),
                ]
            ),
        )
        params = ResolveParams(
            name="fk",
            lookup="dim",
            match={"bk": "natural_id"},
            pk="id",
            effective={"current": {"column": "is_current", "value": "Y"}},  # type: ignore[arg-type]
            on_duplicate="first",
        )
        result = apply_resolve(fact, params, dim)
        row = result.df.collect()[0]
        assert row["fk"] == 1

    def test_date_range(self, spark: SparkSession):
        """Date range effective filters to matching interval."""
        from datetime import date

        from pyspark.sql.types import DateType

        fact = spark.createDataFrame(
            [("A", date(2025, 6, 15))],
            StructType(
                [
                    StructField("bk", StringType(), True),
                    StructField("order_date", DateType(), True),
                ]
            ),
        )
        dim = spark.createDataFrame(
            [
                (1, "A", date(2025, 1, 1), date(2025, 6, 1)),
                (2, "A", date(2025, 6, 1), date(2026, 1, 1)),
            ],
            StructType(
                [
                    StructField("id", IntegerType(), False),
                    StructField("natural_id", StringType(), False),
                    StructField("eff_from", DateType(), True),
                    StructField("eff_to", DateType(), True),
                ]
            ),
        )
        params = ResolveParams.model_validate(
            {
                "name": "fk",
                "lookup": "dim",
                "match": {"bk": "natural_id"},
                "pk": "id",
                "effective": {
                    "date_column": "order_date",
                    "from": "eff_from",
                    "to": "eff_to",
                },
                "on_duplicate": "first",
            }
        )
        result = apply_resolve(fact, params, dim)
        row = result.df.collect()[0]
        assert row["fk"] == 2

    def test_date_range_null_to_current(self, spark: SparkSession):
        """Date range with null 'to' matches as current record."""
        from datetime import date

        from pyspark.sql.types import DateType

        fact = spark.createDataFrame(
            [("A", date(2025, 6, 15))],
            StructType(
                [
                    StructField("bk", StringType(), True),
                    StructField("order_date", DateType(), True),
                ]
            ),
        )
        dim = spark.createDataFrame(
            [
                (1, "A", date(2025, 1, 1), date(2025, 6, 1)),
                (2, "A", date(2025, 6, 1), None),
            ],
            StructType(
                [
                    StructField("id", IntegerType(), False),
                    StructField("natural_id", StringType(), False),
                    StructField("eff_from", DateType(), True),
                    StructField("eff_to", DateType(), True),
                ]
            ),
        )
        params = ResolveParams.model_validate(
            {
                "name": "fk",
                "lookup": "dim",
                "match": {"bk": "natural_id"},
                "pk": "id",
                "effective": {
                    "date_column": "order_date",
                    "from": "eff_from",
                    "to": "eff_to",
                },
                "on_duplicate": "first",
            }
        )
        result = apply_resolve(fact, params, dim)
        row = result.df.collect()[0]
        assert row["fk"] == 2

    def test_where_predicate(self, spark: SparkSession):
        """Where predicate filters lookup rows."""
        fact = spark.createDataFrame([("A",)], _fact_schema("bk"))
        dim = spark.createDataFrame(
            [(1, "A", "active"), (2, "A", "inactive")],
            StructType(
                [
                    StructField("id", IntegerType(), False),
                    StructField("natural_id", StringType(), False),
                    StructField("status", StringType(), True),
                ]
            ),
        )
        params = ResolveParams(
            name="fk",
            lookup="dim",
            match={"bk": "natural_id"},
            pk="id",
            where="status = 'active'",
            on_duplicate="first",
        )
        result = apply_resolve(fact, params, dim)
        row = result.df.collect()[0]
        assert row["fk"] == 1

    def test_effective_and_where_composable(self, spark: SparkSession):
        """Effective and where compose with AND semantics."""
        fact = spark.createDataFrame([("A",)], _fact_schema("bk"))
        dim = spark.createDataFrame(
            [
                (1, "A", True, "us"),
                (2, "A", True, "eu"),
                (3, "A", False, "us"),
            ],
            ["id", "natural_id", "is_current", "region"],
        )
        params = ResolveParams(
            name="fk",
            lookup="dim",
            match={"bk": "natural_id"},
            pk="id",
            effective={"current": "is_current"},  # type: ignore[arg-type]
            where="region = 'us'",
            on_duplicate="first",
        )
        result = apply_resolve(fact, params, dim)
        row = result.df.collect()[0]
        assert row["fk"] == 1


# ---------------------------------------------------------------------------
# Batch resolve mode (Task 7)
# ---------------------------------------------------------------------------


class TestApplyResolveBatch:
    """Test batch FK resolution."""

    def test_two_fks_shared_defaults(self, spark: SparkSession):
        """Two FKs with shared pk resolve correctly."""
        from weevr.operations.pipeline.resolve import apply_resolve_batch

        fact = spark.createDataFrame(
            [("A", "X")],
            StructType(
                [
                    StructField("bk1", StringType(), True),
                    StructField("bk2", StringType(), True),
                ]
            ),
        )
        dim1 = spark.createDataFrame(
            [(10, "A")],
            StructType(
                [
                    StructField("id", IntegerType(), False),
                    StructField("natural_id", StringType(), False),
                ]
            ),
        )
        dim2 = spark.createDataFrame(
            [(20, "X")],
            StructType(
                [
                    StructField("id", IntegerType(), False),
                    StructField("code", StringType(), False),
                ]
            ),
        )
        params = ResolveParams(
            pk="id",
            batch=[  # type: ignore[list-item]
                {"name": "fk1", "lookup": "dim1", "match": {"bk1": "natural_id"}},
                {"name": "fk2", "lookup": "dim2", "match": {"bk2": "code"}},
            ],
        )
        lookups = {"dim1": dim1, "dim2": dim2}
        result = apply_resolve_batch(fact, params, lookups)
        row = result.df.collect()[0]
        assert row["fk1"] == 10
        assert row["fk2"] == 20

    def test_batch_source_columns_not_dropped_mid_batch(self, spark: SparkSession):
        """Source columns shared across FKs survive until batch completes."""
        from weevr.operations.pipeline.resolve import apply_resolve_batch

        fact = spark.createDataFrame([("A",)], _fact_schema("shared_bk"))
        dim1 = spark.createDataFrame(
            [(10, "A")],
            StructType(
                [
                    StructField("id", IntegerType(), False),
                    StructField("natural_id", StringType(), False),
                ]
            ),
        )
        dim2 = spark.createDataFrame(
            [(20, "A")],
            StructType(
                [
                    StructField("id", IntegerType(), False),
                    StructField("bk", StringType(), False),
                ]
            ),
        )
        params = ResolveParams(
            pk="id",
            drop_source_columns=True,
            batch=[  # type: ignore[list-item]
                {"name": "fk1", "lookup": "dim1", "match": {"shared_bk": "natural_id"}},
                {"name": "fk2", "lookup": "dim2", "match": {"shared_bk": "bk"}},
            ],
        )
        lookups = {"dim1": dim1, "dim2": dim2}
        result = apply_resolve_batch(fact, params, lookups)
        row = result.df.collect()[0]
        assert row["fk1"] == 10
        assert row["fk2"] == 20
        assert "shared_bk" not in result.df.columns

    def test_batch_per_item_stats(self, spark: SparkSession):
        """Batch metadata contains per-item stats."""
        from weevr.operations.pipeline.resolve import apply_resolve_batch

        fact = spark.createDataFrame([("A",)], _fact_schema("bk"))
        dim = _dim_df(spark)
        params = ResolveParams(
            pk="id",
            batch=[  # type: ignore[list-item]
                {"name": "fk1", "lookup": "dim", "match": {"bk": "natural_id"}},
            ],
        )
        lookups = {"dim": dim}
        result = apply_resolve_batch(fact, params, lookups)
        stats = result.metadata.get("resolve_stats", {})
        assert "fk1" in stats
        assert stats["fk1"]["total"] == 1


# ---------------------------------------------------------------------------
# on_failure handling via pipeline dispatch (Task 9)
# ---------------------------------------------------------------------------


class TestResolveOnFailure:
    """Test on_failure handling for missing lookups."""

    def test_on_failure_abort_raises(self, spark: SparkSession):
        """on_failure=abort raises when lookup not found."""
        from pydantic import TypeAdapter

        from weevr.operations.pipeline import run_pipeline

        adapter: TypeAdapter[Step] = TypeAdapter(Step)  # type: ignore[type-arg]
        step = adapter.validate_python(
            {"resolve": {"name": "fk", "lookup": "missing", "match": "bk", "pk": "id"}}
        )
        fact = spark.createDataFrame([("A",)], _fact_schema("bk"))
        with pytest.raises(Exception, match="not found"):
            run_pipeline(fact, [step], {}, lookups={})

    def test_on_failure_warn_assigns_sentinel(self, spark: SparkSession):
        """on_failure=warn assigns on_unknown sentinel when lookup missing."""
        from pydantic import TypeAdapter

        from weevr.operations.pipeline import run_pipeline

        adapter: TypeAdapter[Step] = TypeAdapter(Step)  # type: ignore[type-arg]
        step = adapter.validate_python(
            {
                "resolve": {
                    "name": "fk",
                    "lookup": "missing",
                    "match": "bk",
                    "pk": "id",
                    "on_failure": "warn",
                    "on_unknown": -1,
                }
            }
        )
        fact = spark.createDataFrame([("A",), ("B",)], _fact_schema("bk"))
        result = run_pipeline(fact, [step], {}, lookups={})
        rows = result.collect()
        assert all(r["fk"] == -1 for r in rows)


# ---------------------------------------------------------------------------
# Edge case tests (Task 12)
# ---------------------------------------------------------------------------


class TestResolveEdgeCases:
    """Edge case tests for resolve correctness."""

    def test_empty_fact_dataframe(self, spark: SparkSession):
        """Empty fact DF produces empty result with zero stats."""
        fact = spark.createDataFrame([], _fact_schema("bk"))
        dim = _dim_df(spark)
        params = ResolveParams(
            name="fk",
            lookup="dim",
            match={"bk": "natural_id"},
            pk="id",
        )
        result = apply_resolve(fact, params, dim)
        assert result.df.count() == 0
        stats = result.metadata["resolve_stats"]["fk"]
        assert stats["total"] == 0
        assert stats["matched"] == 0

    def test_empty_lookup_all_unknown(self, spark: SparkSession):
        """Empty lookup DF produces all on_unknown sentinels."""
        fact = spark.createDataFrame([("A",), ("B",)], _fact_schema("bk"))
        dim = spark.createDataFrame(
            [],
            StructType(
                [
                    StructField("id", IntegerType(), False),
                    StructField("natural_id", StringType(), False),
                ]
            ),
        )
        params = ResolveParams(
            name="fk",
            lookup="dim",
            match={"bk": "natural_id"},
            pk="id",
            on_unknown=-1,
        )
        result = apply_resolve(fact, params, dim)
        rows = result.df.collect()
        assert len(rows) == 2
        assert all(r["fk"] == -1 for r in rows)

    def test_all_bks_invalid(self, spark: SparkSession):
        """All null BKs produce all on_invalid sentinels."""
        fact = spark.createDataFrame([(None,), (None,)], _fact_schema("bk"))
        dim = _dim_df(spark)
        params = ResolveParams(
            name="fk",
            lookup="dim",
            match={"bk": "natural_id"},
            pk="id",
            on_invalid=-4,
        )
        result = apply_resolve(fact, params, dim)
        rows = result.df.collect()
        assert all(r["fk"] == -4 for r in rows)
        stats = result.metadata["resolve_stats"]["fk"]
        assert stats["invalid"] == 2

    def test_all_bks_match(self, spark: SparkSession):
        """100% match rate when all BKs exist in lookup."""
        fact = spark.createDataFrame([("A",), ("B",), ("C",)], _fact_schema("bk"))
        dim = _dim_df(spark)
        params = ResolveParams(
            name="fk",
            lookup="dim",
            match={"bk": "natural_id"},
            pk="id",
        )
        result = apply_resolve(fact, params, dim)
        stats = result.metadata["resolve_stats"]["fk"]
        assert stats["matched"] == 3
        assert stats["unknown"] == 0
        assert stats["invalid"] == 0
        assert stats["match_rate"] == 100.0

    def test_single_row_fact_single_row_dim(self, spark: SparkSession):
        """Single row fact + single row dim resolves correctly."""
        fact = spark.createDataFrame([("A",)], _fact_schema("bk"))
        dim = spark.createDataFrame(
            [(99, "A")],
            StructType(
                [
                    StructField("id", IntegerType(), False),
                    StructField("natural_id", StringType(), False),
                ]
            ),
        )
        params = ResolveParams(
            name="fk",
            lookup="dim",
            match={"bk": "natural_id"},
            pk="id",
        )
        result = apply_resolve(fact, params, dim)
        assert result.df.count() == 1
        assert result.df.collect()[0]["fk"] == 99

    def test_compound_bk_partial_null(self, spark: SparkSession):
        """Compound BK with one null column in a pair gets on_invalid."""
        fact = spark.createDataFrame(
            [("A", None), ("A", "1")],
            StructType(
                [
                    StructField("region", StringType(), True),
                    StructField("plant", StringType(), True),
                ]
            ),
        )
        dim = spark.createDataFrame(
            [(10, "A", "1")],
            StructType(
                [
                    StructField("id", IntegerType(), False),
                    StructField("region_code", StringType(), False),
                    StructField("plant_code", StringType(), False),
                ]
            ),
        )
        params = ResolveParams(
            name="fk",
            lookup="dim",
            match={"region": "region_code", "plant": "plant_code"},
            pk="id",
            on_invalid=-4,
        )
        result = apply_resolve(fact, params, dim)
        rows = result.df.collect()
        partial_null = [r for r in rows if r["plant"] is None]
        matched = [r for r in rows if r["plant"] == "1"]
        assert partial_null[0]["fk"] == -4
        assert matched[0]["fk"] == 10

    def test_include_column_collision_raises(self, spark: SparkSession):
        """Include column that collides with fact column raises."""
        fact = spark.createDataFrame(
            [("A", "existing")],
            StructType(
                [
                    StructField("bk", StringType(), True),
                    StructField("description", StringType(), True),
                ]
            ),
        )
        dim = spark.createDataFrame(
            [(1, "A", "Plant Alpha")],
            StructType(
                [
                    StructField("id", IntegerType(), False),
                    StructField("natural_id", StringType(), False),
                    StructField("description", StringType(), True),
                ]
            ),
        )
        params = ResolveParams(
            name="fk",
            lookup="dim",
            match={"bk": "natural_id"},
            pk="id",
            include=["description"],
        )
        with pytest.raises(ExecutionError, match="[Cc]ollid"):
            apply_resolve(fact, params, dim)


# ---------------------------------------------------------------------------
# Observed (lazy) statistics
# ---------------------------------------------------------------------------


class TestObservedResolveStats:
    """Observation-mode stats match the eager values exactly."""

    @staticmethod
    def _mixed_fact(spark: SparkSession):
        # A matches, X is unknown, null/blank are invalid
        return spark.createDataFrame(
            [("A",), ("A",), ("X",), (None,), ("",)],
            _fact_schema("nid"),
        )

    @staticmethod
    def _params() -> ResolveParams:
        return ResolveParams(name="fk", lookup="dim", match={"nid": "natural_id"}, pk="id")

    def test_parity_with_eager_stats(self, spark: SparkSession) -> None:
        from weevr.operations.pipeline import ObservationRegistry

        fact = self._mixed_fact(spark)
        dim = _dim_df(spark)

        eager = apply_resolve(fact, self._params(), dim)
        eager_stats = eager.metadata["resolve_stats"]["fk"]

        registry = ObservationRegistry(scope="main")
        observed = apply_resolve(fact, self._params(), dim, observations=registry)
        assert observed.metadata == {}  # stats no longer ride StepResult

        observed.df.collect()  # the terminal action fulfills the observation
        stats = registry.harvest()
        assert stats["main.resolve.fk"] == eager_stats

    def test_parity_when_plan_executes_twice(self, spark: SparkSession) -> None:
        # A pre-write action on the same plan must not corrupt the values —
        # the observed node is fixed, so both executions yield the same row.
        from weevr.operations.pipeline import ObservationRegistry

        fact = self._mixed_fact(spark)
        dim = _dim_df(spark)

        registry = ObservationRegistry(scope="main")
        observed = apply_resolve(fact, self._params(), dim, observations=registry)
        observed.df.count()  # first execution (e.g. rows_after_transforms)
        observed.df.collect()  # second execution (e.g. the write)

        stats = registry.harvest()
        assert stats["main.resolve.fk"]["total"] == 5
        assert stats["main.resolve.fk"]["matched"] == 2
        assert stats["main.resolve.fk"]["unknown"] == 1
        assert stats["main.resolve.fk"]["invalid"] == 2

    def test_harvest_without_action_degrades(self, spark: SparkSession) -> None:
        # No action ever executes the plan: harvest logs and yields absent
        # stats, never raises. Uses a tiny frame so the timeout path is not
        # exercised — the observation is simply unfulfilled.
        from weevr.operations.pipeline import ObservationRegistry, _observations

        registry = ObservationRegistry(scope="main")
        fact = self._mixed_fact(spark)
        dim = _dim_df(spark)
        apply_resolve(fact, self._params(), dim, observations=registry)

        original = _observations._HARVEST_TIMEOUT_S
        _observations._HARVEST_TIMEOUT_S = 0.5
        try:
            stats = registry.harvest()
        finally:
            _observations._HARVEST_TIMEOUT_S = original
        assert stats == {}

    def test_unique_names_for_duplicate_steps(self, spark: SparkSession) -> None:
        from weevr.operations.pipeline import ObservationRegistry

        registry = ObservationRegistry(scope="main")
        fact = self._mixed_fact(spark)
        dim = _dim_df(spark)

        first = apply_resolve(fact, self._params(), dim, observations=registry)
        second = apply_resolve(first.df.drop("fk"), self._params(), dim, observations=registry)
        second.df.collect()

        stats = registry.harvest()
        # Same step/name registered twice: both harvested under distinct keys
        assert set(stats) == {"main.resolve.fk", "main.resolve.fk_1"}


# ---------------------------------------------------------------------------
# Duplicate-gate semantics matrix (equivalence lock)
# ---------------------------------------------------------------------------


class TestDuplicateGateSemantics:
    """Locks on_duplicate outcomes across clean/dup-key/effective cases.

    Written green against the eager fact-side gate; the lookup-side
    pre-check rework must keep every outcome identical.
    """

    @staticmethod
    def _dup_dim(spark: SparkSession):
        # 'A' unique, 'B' duplicated lookup-side
        return spark.createDataFrame(
            [(1, "A"), (2, "B"), (3, "B")],
            StructType(
                [
                    StructField("id", IntegerType(), False),
                    StructField("natural_id", StringType(), False),
                ]
            ),
        )

    @staticmethod
    def _params(on_duplicate: str) -> ResolveParams:
        return ResolveParams(
            name="fk",
            lookup="dim",
            match={"bk": "natural_id"},
            pk="id",
            on_duplicate=on_duplicate,  # type: ignore[arg-type]
        )

    @pytest.mark.parametrize("mode", ["error", "warn", "first"])
    def test_clean_lookup_never_triggers(self, spark: SparkSession, mode: str) -> None:
        fact = spark.createDataFrame([("A",), ("C",)], _fact_schema("bk"))
        result = apply_resolve(fact, self._params(mode), _dim_df(spark))
        by_bk = {r["bk"]: r["fk"] for r in result.df.collect()}
        assert by_bk == {"A": 1, "C": 3}

    def test_error_fires_only_on_actual_fact_match(self, spark: SparkSession) -> None:
        # The lookup carries duplicated 'B' keys, but no fact row references
        # 'B' — error must NOT fire; 'A' resolves normally.
        fact = spark.createDataFrame([("A",)], _fact_schema("bk"))
        result = apply_resolve(fact, self._params("error"), self._dup_dim(spark))
        rows = result.df.collect()
        assert len(rows) == 1
        assert rows[0]["fk"] == 1

    def test_error_fires_on_matched_duplicate(self, spark: SparkSession) -> None:
        fact = spark.createDataFrame([("B",)], _fact_schema("bk"))
        with pytest.raises(Exception, match="[Dd]uplicate"):
            apply_resolve(fact, self._params("error"), self._dup_dim(spark))

    def test_warn_takes_first_and_keeps_row_count(self, spark: SparkSession) -> None:
        fact = spark.createDataFrame([("A",), ("B",)], _fact_schema("bk"))
        result = apply_resolve(fact, self._params("warn"), self._dup_dim(spark))
        rows = result.df.collect()
        assert len(rows) == 2
        by_bk = {r["bk"]: r["fk"] for r in rows}
        assert by_bk["A"] == 1
        assert by_bk["B"] == 2  # asc_nulls_last: lowest pk wins

    def test_first_silent_on_matched_duplicate(self, spark: SparkSession) -> None:
        fact = spark.createDataFrame([("B",), ("B",)], _fact_schema("bk"))
        result = apply_resolve(fact, self._params("first"), self._dup_dim(spark))
        rows = result.df.collect()
        assert len(rows) == 2
        assert all(r["fk"] == 2 for r in rows)

    def test_effective_date_multi_version_unchanged(self, spark: SparkSession) -> None:
        # A BK legitimately carries two date-ranged versions; a fact date
        # inside one range resolves to that version without dup handling.
        from weevr.model.pipeline import EffectiveConfig

        dim = spark.createDataFrame(
            [
                (1, "A", "2026-01-01", "2026-02-01"),
                (2, "A", "2026-02-01", None),
            ],
            StructType(
                [
                    StructField("id", IntegerType(), False),
                    StructField("natural_id", StringType(), False),
                    StructField("vf", StringType(), False),
                    StructField("vt", StringType(), True),
                ]
            ),
        )
        fact = spark.createDataFrame(
            [("A", "2026-01-15"), ("A", "2026-03-01")],
            _fact_schema("bk", "event_date"),
        )
        params = ResolveParams(
            name="fk",
            lookup="dim",
            match={"bk": "natural_id"},
            pk="id",
            on_duplicate="error",
            effective=EffectiveConfig(date_column="event_date", **{"from": "vf", "to": "vt"}),
        )
        result = apply_resolve(fact, params, dim)
        by_date = {r["event_date"]: r["fk"] for r in result.df.collect()}
        assert by_date == {"2026-01-15": 1, "2026-03-01": 2}


# ---------------------------------------------------------------------------
# Duplicate-gate path selection and unique-key reuse
# ---------------------------------------------------------------------------


class TestDuplicateGatePathSelection:
    """Clean lookups skip the window; reuse skips even the pre-check."""

    @staticmethod
    def _params(**kwargs) -> ResolveParams:  # type: ignore[no-untyped-def]
        base = {"name": "fk", "lookup": "dim", "match": {"bk": "natural_id"}, "pk": "id"}
        return ResolveParams(**{**base, **kwargs})

    @staticmethod
    def _analyzed_plan(df: Any) -> str:
        return str(df._jdf.queryExecution().analyzed())

    def test_clean_lookup_plan_has_no_window(self, spark: SparkSession) -> None:
        fact = spark.createDataFrame([("A",)], _fact_schema("bk"))
        result = apply_resolve(fact, self._params(), _dim_df(spark))
        assert "row_number" not in self._analyzed_plan(result.df)

    def test_dup_lookup_plan_keeps_window(self, spark: SparkSession) -> None:
        dup_dim = spark.createDataFrame(
            [(1, "A"), (2, "A")],
            StructType(
                [
                    StructField("id", IntegerType(), False),
                    StructField("natural_id", StringType(), False),
                ]
            ),
        )
        fact = spark.createDataFrame([("A",)], _fact_schema("bk"))
        result = apply_resolve(fact, self._params(on_duplicate="first"), dup_dim)
        assert "row_number" in self._analyzed_plan(result.df)

    def test_valid_reuse_skips_precheck(self, spark: SparkSession, spark_action_counter) -> None:
        from weevr.operations.pipeline import LookupMeta, ObservationRegistry

        meta = LookupMeta(
            key_columns=("natural_id",), unique_key_checked=True, unique_key_passed=True
        )
        fact = spark.createDataFrame([("A",)], _fact_schema("bk"))
        registry = ObservationRegistry(scope="main")

        start = spark_action_counter["total"]
        apply_resolve(fact, self._params(), _dim_df(spark), observations=registry, lookup_meta=meta)
        # Reuse + observed stats: the resolve step itself fires ZERO actions
        assert spark_action_counter["total"] - start == 0

    def test_normalize_invalidates_reuse(self, spark: SparkSession, spark_action_counter) -> None:
        from weevr.operations.pipeline import LookupMeta, ObservationRegistry

        meta = LookupMeta(
            key_columns=("natural_id",), unique_key_checked=True, unique_key_passed=True
        )
        fact = spark.createDataFrame([("a",)], _fact_schema("bk"))
        registry = ObservationRegistry(scope="main")

        start = spark_action_counter["total"]
        apply_resolve(
            fact,
            self._params(normalize="trim_lower"),
            _dim_df(spark),
            observations=registry,
            lookup_meta=meta,
        )
        # trim_lower can merge distinct keys post-check — the pre-check must run
        assert spark_action_counter["total"] - start == 1

    def test_key_mismatch_invalidates_reuse(
        self, spark: SparkSession, spark_action_counter
    ) -> None:
        from weevr.operations.pipeline import LookupMeta, ObservationRegistry

        meta = LookupMeta(key_columns=("id",), unique_key_checked=True, unique_key_passed=True)
        fact = spark.createDataFrame([("A",)], _fact_schema("bk"))
        registry = ObservationRegistry(scope="main")

        start = spark_action_counter["total"]
        apply_resolve(fact, self._params(), _dim_df(spark), observations=registry, lookup_meta=meta)
        # Declared unique key covers different columns than this join
        assert spark_action_counter["total"] - start == 1


# ---------------------------------------------------------------------------
# Broadcast hint policy
# ---------------------------------------------------------------------------


class TestBroadcastPolicy:
    """Hint present for declared/known-small; absent for unknown size."""

    @staticmethod
    def _params() -> ResolveParams:
        return ResolveParams(name="fk", lookup="dim", match={"bk": "natural_id"}, pk="id")

    @staticmethod
    def _analyzed_plan(df: Any) -> str:
        return str(df._jdf.queryExecution().analyzed())

    def _resolve_with_meta(self, spark: SparkSession, meta):  # type: ignore[no-untyped-def]
        fact = spark.createDataFrame([("A",)], _fact_schema("bk"))
        return apply_resolve(fact, self._params(), _dim_df(spark), lookup_meta=meta)

    def test_declared_broadcast_hints(self, spark: SparkSession) -> None:
        from weevr.operations.pipeline import LookupMeta

        result = self._resolve_with_meta(spark, LookupMeta(broadcast_declared=True))
        assert "broadcast" in self._analyzed_plan(result.df).lower()

    def test_known_small_hints(self, spark: SparkSession) -> None:
        from weevr.operations.pipeline import LookupMeta

        result = self._resolve_with_meta(spark, LookupMeta(size_in_bytes=1024))
        assert "broadcast" in self._analyzed_plan(result.df).lower()

    def test_unknown_size_defers_to_aqe(self, spark: SparkSession) -> None:
        from weevr.operations.pipeline import LookupMeta

        result = self._resolve_with_meta(spark, LookupMeta())
        assert "broadcast" not in self._analyzed_plan(result.df).lower()

    def test_large_size_defers_to_aqe(self, spark: SparkSession) -> None:
        from weevr.operations.pipeline import LookupMeta

        result = self._resolve_with_meta(spark, LookupMeta(size_in_bytes=1 << 40))
        assert "broadcast" not in self._analyzed_plan(result.df).lower()

    def test_results_identical_with_hint(self, spark: SparkSession) -> None:
        from weevr.operations.pipeline import LookupMeta

        fact = spark.createDataFrame([("A",), ("C",), (None,)], _fact_schema("bk"))
        plain = apply_resolve(fact, self._params(), _dim_df(spark))
        hinted = apply_resolve(
            fact, self._params(), _dim_df(spark), lookup_meta=LookupMeta(size_in_bytes=1)
        )
        assert sorted(map(str, plain.df.collect())) == sorted(map(str, hinted.df.collect()))


class TestObservedMapAndFallbackStats:
    """Map validate-mode and missing-lookup fallback stats go lazy."""

    def test_map_unmapped_count_parity(self, spark: SparkSession, spark_action_counter) -> None:
        from weevr.model.pipeline import MapParams
        from weevr.operations.pipeline import ObservationRegistry
        from weevr.operations.pipeline.value_mapping import apply_map

        df = spark.createDataFrame([("A",), ("B",), ("Z",), (None,)], _fact_schema("code"))
        params = MapParams(column="code", values={"A": "1", "B": "2"}, unmapped="validate")

        eager = apply_map(df, params)
        assert eager.metadata["unmapped_count"] == 1  # Z; nulls excluded

        registry = ObservationRegistry(scope="main")
        start = spark_action_counter["total"]
        observed = apply_map(df, params, observations=registry)
        assert spark_action_counter["total"] - start == 0  # no eager action
        assert "unmapped_count" not in observed.metadata

        observed.df.collect()
        stats = registry.harvest()
        assert stats["main.map.code"]["unmapped_count"] == 1

    def test_missing_lookup_fallback_count_observed(
        self, spark: SparkSession, spark_action_counter
    ) -> None:
        from weevr.model.pipeline import ResolveStep
        from weevr.operations.pipeline import ObservationRegistry, run_pipeline

        df = spark.createDataFrame([("A",), ("B",)], _fact_schema("bk"))
        step = ResolveStep(
            resolve=ResolveParams(
                name="fk",
                lookup="missing_dim",
                match={"bk": "natural_id"},
                pk="id",
                on_failure="warn",
            )
        )
        registry = ObservationRegistry(scope="main")
        start = spark_action_counter["total"]
        out = run_pipeline(df, [step], {}, observations=registry)
        assert spark_action_counter["total"] - start == 0  # fallback no longer counts

        out.collect()
        stats = registry.harvest()
        assert stats["main.resolve.fk"] == {
            "total": 2,
            "matched": 0,
            "unknown": 2,
            "invalid": 0,
            "duplicates": 0,
            "match_rate": 0.0,
        }
