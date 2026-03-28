"""Tests for resolve step handler — FK resolution via lookup join."""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)

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
            StructType(
                [
                    StructField("id", IntegerType(), False),
                    StructField("natural_id", StringType(), False),
                    StructField("is_current", StringType(), True),
                ]
            ),
        )
        # Spark reads boolean YAML as string, so use "true"/"false"
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
