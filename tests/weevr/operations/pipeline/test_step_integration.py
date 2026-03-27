"""Integration tests — YAML-to-execution round-trips for M115 step types."""

from datetime import date

import pytest
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from weevr.model.pipeline import Step
from weevr.operations.pipeline import run_pipeline


def _parse_step(raw: dict) -> Step:
    """Parse a raw dict into a typed Step model (simulates YAML loading)."""
    from pydantic import TypeAdapter

    adapter = TypeAdapter(Step)
    return adapter.validate_python(raw)


@pytest.mark.spark
class TestConcatIntegration:
    """YAML round-trip tests for the concat step."""

    def test_concat_yaml_round_trip(self, spark):
        df = spark.createDataFrame(
            [("123 Main St", "Springfield", "IL")],
            ["street", "city", "state"],
        )
        step = _parse_step(
            {
                "concat": {
                    "target": "full_address",
                    "columns": ["street", "city", "state"],
                    "separator": ", ",
                    "null_mode": "skip",
                }
            }
        )
        result = run_pipeline(df, [step], {})
        row = result.collect()[0]
        assert row.full_address == "123 Main St, Springfield, IL"


@pytest.mark.spark
class TestMapIntegration:
    """YAML round-trip tests for the map step."""

    def test_map_yaml_round_trip(self, spark):
        df = spark.createDataFrame(
            [("A",), ("B",), (None,)],
            schema=StructType([StructField("code", StringType())]),
        )
        step = _parse_step(
            {
                "map": {
                    "column": "code",
                    "target": "label",
                    "values": {"A": "Active", "B": "Blocked"},
                    "on_null": "Missing",
                    "default": "Other",
                }
            }
        )
        result = run_pipeline(df, [step], {})
        rows = result.orderBy("label").collect()
        labels = [r.label for r in rows]
        assert "Active" in labels
        assert "Blocked" in labels
        assert "Missing" in labels


@pytest.mark.spark
class TestFillNullTypeDefaultsIntegration:
    """YAML round-trip tests for the fill_null step with type_defaults mode."""

    def test_type_defaults_round_trip(self, spark):
        df = spark.createDataFrame(
            [(None, None, None)],
            schema=StructType(
                [
                    StructField("name", StringType()),
                    StructField("age", IntegerType()),
                    StructField("active", BooleanType()),
                ]
            ),
        )
        step = _parse_step(
            {
                "fill_null": {
                    "mode": "type_defaults",
                    "code": "unknown",
                    "exclude": ["id"],
                }
            }
        )
        result = run_pipeline(df, [step], {})
        row = result.collect()[0]
        assert row.name == "Unknown"
        assert row.age == 0
        assert row.active is False

    def test_composable_round_trip(self, spark):
        df = spark.createDataFrame(
            [(None, None)],
            schema=StructType(
                [
                    StructField("name", StringType()),
                    StructField("special", IntegerType()),
                ]
            ),
        )
        step = _parse_step(
            {
                "fill_null": {
                    "mode": "type_defaults",
                    "code": "unknown",
                    "columns": {"special": -1},
                }
            }
        )
        result = run_pipeline(df, [step], {})
        row = result.collect()[0]
        assert row.name == "Unknown"
        assert row.special == -1


@pytest.mark.spark
class TestFormatIntegration:
    """YAML round-trip tests for the format step."""

    def test_format_yaml_round_trip(self, spark):
        df = spark.createDataFrame(
            [("1234567890", 1234.5, date(2026, 1, 15))],
            schema=StructType(
                [
                    StructField("phone", StringType()),
                    StructField("amount", DoubleType()),
                    StructField("event_date", DateType()),
                ]
            ),
        )
        step = _parse_step(
            {
                "format": {
                    "phone": {
                        "pattern": "({1:3}){4:3}-{7:4}",
                    },
                    "amount_display": {
                        "source": "amount",
                        "number": "#,##0.00",
                    },
                    "event_date": {
                        "date": "yyyy-MM-dd",
                    },
                }
            }
        )
        result = run_pipeline(df, [step], {})
        row = result.collect()[0]
        assert row.phone == "(123)456-7890"
        assert row.amount_display == "1,234.50"
        assert row.event_date == "2026-01-15"


@pytest.mark.spark
class TestFillNullBackwardCompat:
    """Backward compatibility tests for the fill_null step."""

    def test_fill_null_columns_only(self, spark):
        df = spark.createDataFrame(
            [(None, None)],
            schema=StructType(
                [
                    StructField("discount", IntegerType()),
                    StructField("region", StringType()),
                ]
            ),
        )
        step = _parse_step(
            {
                "fill_null": {
                    "columns": {
                        "discount": 0,
                        "region": "Unknown",
                    },
                }
            }
        )
        result = run_pipeline(df, [step], {})
        row = result.collect()[0]
        assert row.discount == 0
        assert row.region == "Unknown"
