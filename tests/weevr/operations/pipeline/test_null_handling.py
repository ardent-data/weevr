"""Tests for null-handling pipeline step handlers — fill_null, coalesce."""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import BooleanType, IntegerType, StringType, StructField, StructType

from weevr.model.pipeline import CoalesceParams, FillNullParams
from weevr.operations.pipeline.null_handling import apply_coalesce, apply_fill_null

pytestmark = pytest.mark.spark


class TestApplyFillNull:
    """Test fill_null step handler."""

    def test_numeric_defaults(self, spark: SparkSession):
        """Null numeric column filled with default."""
        df = spark.createDataFrame([(1, None), (2, 10)], ["id", "amount"])
        params = FillNullParams(columns={"amount": 0})
        result = apply_fill_null(df, params)
        rows = {r["id"]: r["amount"] for r in result.collect()}
        assert rows[1] == 0
        assert rows[2] == 10

    def test_string_defaults(self, spark: SparkSession):
        """Null string column filled with default."""
        df = spark.createDataFrame([(1, None), (2, "alice")], ["id", "name"])
        params = FillNullParams(columns={"name": "unknown"})
        result = apply_fill_null(df, params)
        rows = {r["id"]: r["name"] for r in result.collect()}
        assert rows[1] == "unknown"
        assert rows[2] == "alice"

    def test_no_nulls_unchanged(self, spark: SparkSession):
        """No nulls present → data unchanged."""
        df = spark.createDataFrame([(1, 10), (2, 20)], ["id", "amount"])
        params = FillNullParams(columns={"amount": 0})
        result = apply_fill_null(df, params)
        rows = {r["id"]: r["amount"] for r in result.collect()}
        assert rows[1] == 10
        assert rows[2] == 20


class TestApplyCoalesce:
    """Test coalesce step handler."""

    def test_first_non_null(self, spark: SparkSession):
        """First non-null value selected from source columns."""
        df = spark.createDataFrame(
            [(None, "work@x.com"), ("p@x.com", "w@x.com")],
            ["personal", "work"],
        )
        params = CoalesceParams(columns={"email": ["personal", "work"]})
        result = apply_coalesce(df, params)
        rows = result.collect()
        assert rows[0]["email"] == "work@x.com"
        assert rows[1]["email"] == "p@x.com"

    def test_all_null(self, spark: SparkSession):
        """All sources null → output is null."""
        from pyspark.sql.types import StringType, StructField, StructType

        schema = StructType([StructField("a", StringType()), StructField("b", StringType())])
        df = spark.createDataFrame([(None, None)], schema=schema)
        params = CoalesceParams(columns={"out": ["a", "b"]})
        result = apply_coalesce(df, params)
        assert result.collect()[0]["out"] is None

    def test_new_column(self, spark: SparkSession):
        """Output column name doesn't exist yet → created."""
        df = spark.createDataFrame([(1, 2)], ["a", "b"])
        params = CoalesceParams(columns={"merged": ["a", "b"]})
        result = apply_coalesce(df, params)
        assert "merged" in result.columns
        assert result.collect()[0]["merged"] == 1


class TestFillNullTypeDefaults:
    """Tests for fill_null type-aware mode."""

    def test_type_defaults_unknown_code(self, spark: SparkSession):
        """Type defaults with unknown code fills by DataType."""
        df = spark.createDataFrame(
            [(None, None, None)],
            schema=StructType([
                StructField("name", StringType()),
                StructField("age", IntegerType()),
                StructField("active", BooleanType()),
            ]),
        )
        params = FillNullParams(mode="type_defaults", code="unknown")
        result = apply_fill_null(df, params)
        row = result.collect()[0]
        assert row.name == "Unknown"
        assert row.age == 0
        assert row.active is False

    def test_type_defaults_not_applicable_code(self, spark: SparkSession):
        """Type defaults with not_applicable code fills strings accordingly."""
        df = spark.createDataFrame(
            [(None,)],
            schema=StructType([StructField("name", StringType())]),
        )
        params = FillNullParams(mode="type_defaults", code="not_applicable")
        result = apply_fill_null(df, params)
        assert result.collect()[0].name == "Not Applicable"

    def test_type_defaults_invalid_code(self, spark: SparkSession):
        """Type defaults with invalid code fills strings accordingly."""
        df = spark.createDataFrame(
            [(None,)],
            schema=StructType([StructField("name", StringType())]),
        )
        params = FillNullParams(mode="type_defaults", code="invalid")
        result = apply_fill_null(df, params)
        assert result.collect()[0].name == "Invalid"

    def test_type_defaults_include_glob(self, spark: SparkSession):
        """Include glob restricts which columns receive type defaults."""
        df = spark.createDataFrame(
            [(None, None, None)],
            schema=StructType([
                StructField("addr_line1", StringType()),
                StructField("addr_line2", StringType()),
                StructField("city", StringType()),
            ]),
        )
        params = FillNullParams(mode="type_defaults", code="unknown", include=["addr_*"])
        result = apply_fill_null(df, params)
        row = result.collect()[0]
        assert row.addr_line1 == "Unknown"
        assert row.addr_line2 == "Unknown"
        assert row.city is None  # Not included

    def test_type_defaults_exclude_glob(self, spark: SparkSession):
        """Exclude glob prevents specific columns from receiving type defaults."""
        df = spark.createDataFrame(
            [(None, None)],
            schema=StructType([
                StructField("id", IntegerType()),
                StructField("amount", IntegerType()),
            ]),
        )
        params = FillNullParams(mode="type_defaults", code="unknown", exclude=["id"])
        result = apply_fill_null(df, params)
        row = result.collect()[0]
        assert row.id is None  # Excluded
        assert row.amount == 0

    def test_type_defaults_overrides(self, spark: SparkSession):
        """Per-column overrides replace type-based defaults for named columns."""
        df = spark.createDataFrame(
            [(None, None)],
            schema=StructType([
                StructField("region", StringType()),
                StructField("city", StringType()),
            ]),
        )
        params = FillNullParams(
            mode="type_defaults",
            code="unknown",
            overrides={"region": "Unspecified"},
        )
        result = apply_fill_null(df, params)
        row = result.collect()[0]
        assert row.region == "Unspecified"
        assert row.city == "Unknown"

    def test_type_defaults_where_predicate(self, spark: SparkSession):
        """Where predicate applies fills only to rows matching the condition."""
        df = spark.createDataFrame(
            [("UNKNOWN", None), ("VALID", None)],
            schema=StructType([
                StructField("status", StringType()),
                StructField("name", StringType()),
            ]),
        )
        params = FillNullParams(
            mode="type_defaults",
            code="unknown",
            where="status = 'UNKNOWN'",
        )
        result = apply_fill_null(df, params)
        rows = sorted(result.collect(), key=lambda r: r.status)
        # UNKNOWN status row gets fill
        assert rows[0].name == "Unknown"
        # VALID status row stays NULL
        assert rows[1].name is None

    def test_composable_mode(self, spark: SparkSession):
        """type_defaults and explicit columns apply in a single step."""
        df = spark.createDataFrame(
            [(None, None)],
            schema=StructType([
                StructField("name", StringType()),
                StructField("special", IntegerType()),
            ]),
        )
        params = FillNullParams(
            mode="type_defaults",
            code="unknown",
            columns={"special": -1},
        )
        result = apply_fill_null(df, params)
        row = result.collect()[0]
        assert row.name == "Unknown"  # From type_defaults
        assert row.special == -1      # From explicit columns override

    def test_backward_compat_columns_only(self, spark: SparkSession):
        """Existing columns-only fill behavior is unchanged."""
        df = spark.createDataFrame(
            [(None, None)],
            schema=StructType([
                StructField("discount", IntegerType()),
                StructField("region", StringType()),
            ]),
        )
        params = FillNullParams(columns={"discount": 0, "region": "Unknown"})
        result = apply_fill_null(df, params)
        row = result.collect()[0]
        assert row.discount == 0
        assert row.region == "Unknown"
