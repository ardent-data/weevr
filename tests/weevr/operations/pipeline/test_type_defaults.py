"""Tests for the type-aware fill-value resolver."""

from datetime import date, datetime
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from weevr.operations.pipeline.type_defaults import resolve_type_defaults


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("test_type_defaults").getOrCreate()  # type: ignore[attr-defined]


def _make_df(spark, fields):
    """Create an empty DataFrame with the given schema fields."""
    schema = StructType(fields)
    return spark.createDataFrame([], schema)


@pytest.mark.spark
class TestResolveTypeDefaults:
    """Tests for resolve_type_defaults()."""

    def test_unknown_code_string_type(self, spark):
        df = _make_df(spark, [StructField("name", StringType())])
        result = resolve_type_defaults(df, "unknown")
        assert result == {"name": "Unknown"}

    def test_unknown_code_integer_type(self, spark):
        df = _make_df(spark, [StructField("count", IntegerType())])
        result = resolve_type_defaults(df, "unknown")
        assert result == {"count": 0}

    def test_unknown_code_long_type(self, spark):
        df = _make_df(spark, [StructField("big_id", LongType())])
        result = resolve_type_defaults(df, "unknown")
        assert result == {"big_id": 0}

    def test_unknown_code_boolean_type(self, spark):
        df = _make_df(spark, [StructField("flag", BooleanType())])
        result = resolve_type_defaults(df, "unknown")
        assert result == {"flag": False}

    def test_unknown_code_float_type(self, spark):
        df = _make_df(spark, [StructField("ratio", FloatType())])
        result = resolve_type_defaults(df, "unknown")
        assert result == {"ratio": 0.0}

    def test_unknown_code_double_type(self, spark):
        df = _make_df(spark, [StructField("amount", DoubleType())])
        result = resolve_type_defaults(df, "unknown")
        assert result == {"amount": 0.0}

    def test_unknown_code_decimal_type(self, spark):
        df = _make_df(spark, [StructField("price", DecimalType(10, 2))])
        result = resolve_type_defaults(df, "unknown")
        assert result == {"price": Decimal(0)}

    def test_unknown_code_date_type(self, spark):
        df = _make_df(spark, [StructField("created", DateType())])
        result = resolve_type_defaults(df, "unknown")
        assert result == {"created": date(1970, 1, 1)}

    def test_unknown_code_timestamp_type(self, spark):
        df = _make_df(spark, [StructField("updated", TimestampType())])
        result = resolve_type_defaults(df, "unknown")
        assert result == {"updated": datetime(1970, 1, 1)}

    def test_not_applicable_code_string(self, spark):
        df = _make_df(spark, [StructField("name", StringType())])
        result = resolve_type_defaults(df, "not_applicable")
        assert result == {"name": "Not Applicable"}

    def test_invalid_code_string(self, spark):
        df = _make_df(spark, [StructField("name", StringType())])
        result = resolve_type_defaults(df, "invalid")
        assert result == {"name": "Invalid"}

    def test_complex_type_skipped(self, spark):
        df = _make_df(
            spark,
            [
                StructField("tags", ArrayType(StringType())),
                StructField("attrs", MapType(StringType(), StringType())),
                StructField("name", StringType()),
            ],
        )
        result = resolve_type_defaults(df, "unknown")
        assert result == {"name": "Unknown"}
        assert "tags" not in result
        assert "attrs" not in result

    def test_struct_type_skipped(self, spark):
        df = _make_df(
            spark,
            [
                StructField("nested", StructType([StructField("x", IntegerType())])),
                StructField("val", IntegerType()),
            ],
        )
        result = resolve_type_defaults(df, "unknown")
        assert result == {"val": 0}

    def test_include_glob_pattern(self, spark):
        df = _make_df(
            spark,
            [
                StructField("city", StringType()),
                StructField("addr_line1", StringType()),
                StructField("addr_line2", StringType()),
                StructField("zip_code", StringType()),
            ],
        )
        result = resolve_type_defaults(df, "unknown", include=["addr_*"])
        assert result == {"addr_line1": "Unknown", "addr_line2": "Unknown"}

    def test_exclude_glob_pattern(self, spark):
        df = _make_df(
            spark,
            [
                StructField("id", IntegerType()),
                StructField("customer_id", IntegerType()),
                StructField("amount", IntegerType()),
            ],
        )
        result = resolve_type_defaults(df, "unknown", exclude=["*_id", "id"])
        assert result == {"amount": 0}

    def test_include_and_exclude_compose(self, spark):
        df = _make_df(
            spark,
            [
                StructField("addr_id", IntegerType()),
                StructField("addr_line1", StringType()),
                StructField("addr_zip", StringType()),
            ],
        )
        result = resolve_type_defaults(df, "unknown", include=["addr_*"], exclude=["*_id"])
        assert result == {"addr_line1": "Unknown", "addr_zip": "Unknown"}

    def test_overrides_replace_defaults(self, spark):
        df = _make_df(
            spark,
            [
                StructField("region", StringType()),
                StructField("city", StringType()),
            ],
        )
        result = resolve_type_defaults(df, "unknown", overrides={"region": "Unspecified"})
        assert result == {"region": "Unspecified", "city": "Unknown"}

    def test_multiple_types_in_schema(self, spark):
        df = _make_df(
            spark,
            [
                StructField("name", StringType()),
                StructField("age", IntegerType()),
                StructField("active", BooleanType()),
                StructField("score", DoubleType()),
            ],
        )
        result = resolve_type_defaults(df, "unknown")
        assert result == {"name": "Unknown", "age": 0, "active": False, "score": 0.0}

    def test_empty_schema_returns_empty(self, spark):
        df = _make_df(spark, [])
        result = resolve_type_defaults(df, "unknown")
        assert result == {}
