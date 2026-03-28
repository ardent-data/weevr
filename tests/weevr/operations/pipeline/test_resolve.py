"""Tests for resolve step handler — FK resolution via lookup join."""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from weevr.model.pipeline import ResolveParams
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
