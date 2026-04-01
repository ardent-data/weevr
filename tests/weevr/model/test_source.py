"""Tests for Source and DedupConfig models."""

import pytest
from pydantic import ValidationError

from weevr.model.source import DedupConfig, Source


class TestDedupConfig:
    """Test DedupConfig model."""

    def test_minimal_dedup(self):
        """DedupConfig with only required keys field."""
        d = DedupConfig(keys=["customer_id"])
        assert d.keys == ["customer_id"]
        assert d.order_by is None

    def test_full_dedup(self):
        """DedupConfig with all fields."""
        d = DedupConfig(keys=["id", "date"], order_by="modified_date")
        assert d.order_by == "modified_date"

    def test_frozen(self):
        """DedupConfig is immutable."""
        d = DedupConfig(keys=["id"])
        with pytest.raises(ValidationError):
            d.keys = ["other"]  # type: ignore[misc]

    def test_round_trip(self):
        """DedupConfig round-trips through model_dump/model_validate."""
        d = DedupConfig(keys=["id"], order_by="ts")
        assert DedupConfig.model_validate(d.model_dump()) == d


class TestSource:
    """Test Source model."""

    def test_valid_delta_source(self):
        """Delta source requires alias."""
        s = Source(type="delta", alias="raw.customers")
        assert s.type == "delta"
        assert s.alias == "raw.customers"

    def test_valid_csv_source(self):
        """CSV source requires path."""
        s = Source(type="csv", path="/data/files/orders.csv")
        assert s.path == "/data/files/orders.csv"

    def test_valid_json_source(self):
        """JSON source requires path."""
        s = Source(type="json", path="/data/raw/events/")
        assert s.type == "json"

    def test_valid_parquet_source(self):
        """Parquet source requires path."""
        s = Source(type="parquet", path="/mnt/data/")
        assert s.type == "parquet"

    def test_delta_missing_alias_raises(self):
        """Delta source without alias raises ValidationError."""
        with pytest.raises(ValidationError, match="alias"):
            Source(type="delta")

    def test_csv_missing_path_raises(self):
        """CSV source without path raises ValidationError."""
        with pytest.raises(ValidationError, match="path"):
            Source(type="csv")

    def test_json_missing_path_raises(self):
        """JSON source without path raises ValidationError."""
        with pytest.raises(ValidationError, match="path"):
            Source(type="json")

    def test_parquet_missing_path_raises(self):
        """Parquet source without path raises ValidationError."""
        with pytest.raises(ValidationError, match="path"):
            Source(type="parquet")

    def test_custom_type_no_alias_or_path(self):
        """Custom source type passes without alias or path requirement."""
        s = Source(type="api")
        assert s.type == "api"
        assert s.alias is None
        assert s.path is None

    def test_source_with_options(self):
        """Source accepts arbitrary options dict."""
        s = Source(type="csv", path="/data/", options={"delimiter": "|", "header": True})
        assert s.options["delimiter"] == "|"

    def test_source_with_dedup(self):
        """Source accepts nested DedupConfig."""
        s = Source(
            type="delta",
            alias="raw.customers",
            dedup={"keys": ["id"], "order_by": "modified_at"},  # type: ignore[arg-type]
        )
        assert isinstance(s.dedup, DedupConfig)
        assert s.dedup.keys == ["id"]

    def test_frozen(self):
        """Source is immutable."""
        s = Source(type="delta", alias="raw.orders")
        with pytest.raises(ValidationError):
            s.type = "csv"  # type: ignore[misc]

    def test_round_trip(self):
        """Source round-trips through model_dump/model_validate."""
        s = Source(
            type="delta",
            alias="raw.customers",
            options={"timeout": 30},
            dedup={"keys": ["id"]},  # type: ignore[arg-type]
        )
        assert Source.model_validate(s.model_dump()) == s

    def test_default_options_is_empty_dict(self):
        """options defaults to empty dict, not None."""
        s = Source(type="api")
        assert s.options == {}


class TestSourceLookupReference:
    """Test Source with lookup references."""

    def test_lookup_only(self):
        """Lookup-only source is valid."""
        s = Source(lookup="ref_categories")
        assert s.lookup == "ref_categories"
        assert s.type is None

    def test_lookup_rejects_type(self):
        """Lookup source with type set is rejected."""
        with pytest.raises(ValidationError, match="lookup sources must not set 'type'"):
            Source(lookup="ref_categories", type="delta")

    def test_no_type_no_lookup_rejected(self):
        """Source without type or lookup is rejected."""
        with pytest.raises(ValidationError, match="require either 'type' or 'lookup'"):
            Source()

    def test_lookup_from_dict(self):
        """Construct lookup source from dict (YAML-like)."""
        s = Source.model_validate({"lookup": "dim_region"})
        assert s.lookup == "dim_region"
        assert s.type is None


class TestSourceConnectionReference:
    """Test Source with connection-based lakehouse references."""

    def test_connection_with_table(self):
        """Source with connection and table parses correctly; type is optional."""
        s = Source(connection="my_lakehouse", table="customers")
        assert s.connection == "my_lakehouse"
        assert s.table == "customers"
        assert s.type is None

    def test_connection_with_table_and_schema(self):
        """Source with connection, table, and schema override parses correctly."""
        s = Source.model_validate(
            {"connection": "my_lakehouse", "table": "orders", "schema": "sales"}
        )
        assert s.connection == "my_lakehouse"
        assert s.table == "orders"
        assert s.schema_override == "sales"

    def test_connection_and_alias_mutually_exclusive(self):
        """Setting both connection and alias raises ValidationError."""
        with pytest.raises(ValidationError, match="connection"):
            Source(connection="my_lakehouse", table="customers", alias="raw.customers")

    def test_connection_without_table_raises(self):
        """connection without table raises ValidationError."""
        with pytest.raises(ValidationError, match="table"):
            Source(connection="my_lakehouse")

    def test_table_without_connection_raises(self):
        """table without connection raises ValidationError."""
        with pytest.raises(ValidationError, match="connection"):
            Source(type="delta", alias="raw.customers", table="customers")

    def test_connection_with_explicit_delta_type(self):
        """connection with explicit type='delta' parses correctly."""
        s = Source(connection="my_lakehouse", table="events", type="delta")
        assert s.type == "delta"
        assert s.connection == "my_lakehouse"

    def test_connection_with_non_delta_type_raises(self):
        """connection with a file-based type raises ValidationError."""
        with pytest.raises(ValidationError, match="connection"):
            Source(connection="my_lakehouse", table="events", type="csv")


class TestSourceGeneratedTypes:
    """Test Source with generated sequence types."""

    def test_date_sequence_parses(self):
        """date_sequence source with required fields parses correctly."""
        s = Source(type="date_sequence", start="2024-01-01", end="2024-12-31", column="date")
        assert s.type == "date_sequence"
        assert s.start == "2024-01-01"
        assert s.end == "2024-12-31"
        assert s.column == "date"
        assert s.step is None

    def test_date_sequence_with_step_parses(self):
        """date_sequence source with step parses correctly."""
        s = Source(
            type="date_sequence",
            start="2024-01-01",
            end="2024-12-31",
            column="date",
            step="month",
        )
        assert s.step == "month"

    def test_int_sequence_parses(self):
        """int_sequence source with required fields parses correctly."""
        s = Source(type="int_sequence", start=1, end=100, column="id")
        assert s.type == "int_sequence"
        assert s.start == 1
        assert s.end == 100
        assert s.column == "id"
        assert s.step is None

    def test_int_sequence_with_step_parses(self):
        """int_sequence source with step parses correctly."""
        s = Source(type="int_sequence", start=1, end=100, column="id", step=5)
        assert s.step == 5

    def test_generated_missing_column_raises(self):
        """Generated type without column raises ValidationError."""
        with pytest.raises(ValidationError, match="column"):
            Source(type="date_sequence", start="2024-01-01", end="2024-12-31")

    def test_generated_missing_start_raises(self):
        """Generated type without start raises ValidationError."""
        with pytest.raises(ValidationError, match="start"):
            Source(type="date_sequence", end="2024-12-31", column="date")

    def test_generated_missing_end_raises(self):
        """Generated type without end raises ValidationError."""
        with pytest.raises(ValidationError, match="end"):
            Source(type="date_sequence", start="2024-01-01", column="date")

    def test_date_sequence_with_alias_raises(self):
        """date_sequence with alias set is rejected."""
        with pytest.raises(ValidationError, match="alias"):
            Source(
                type="date_sequence",
                start="2024-01-01",
                end="2024-12-31",
                column="date",
                alias="some.alias",
            )

    def test_date_sequence_with_path_raises(self):
        """date_sequence with path set is rejected."""
        with pytest.raises(ValidationError, match="path"):
            Source(
                type="date_sequence",
                start="2024-01-01",
                end="2024-12-31",
                column="date",
                path="/some/path",
            )

    def test_date_sequence_with_nonempty_options_raises(self):
        """date_sequence with non-empty options is rejected."""
        with pytest.raises(ValidationError, match="options"):
            Source(
                type="date_sequence",
                start="2024-01-01",
                end="2024-12-31",
                column="date",
                options={"k": "v"},
            )

    def test_date_sequence_with_empty_options_accepted(self):
        """date_sequence with empty options dict is accepted."""
        s = Source(
            type="date_sequence",
            start="2024-01-01",
            end="2024-12-31",
            column="date",
            options={},
        )
        assert s.options == {}

    def test_date_sequence_with_dedup_raises(self):
        """date_sequence with dedup set is rejected."""
        with pytest.raises(ValidationError, match="dedup"):
            Source(
                type="date_sequence",
                start="2024-01-01",
                end="2024-12-31",
                column="date",
                dedup={"keys": ["date"]},  # type: ignore[arg-type]
            )

    def test_date_sequence_with_connection_raises(self):
        """date_sequence with connection set is rejected; rejected by connection branch."""
        with pytest.raises(ValidationError, match="connection sources only support"):
            Source(
                type="date_sequence",
                start="2024-01-01",
                end="2024-12-31",
                column="date",
                connection="my_lakehouse",
                table="t",
            )

    def test_date_sequence_invalid_step_raises(self):
        """date_sequence with step not in allowed values is rejected."""
        with pytest.raises(ValidationError, match="step"):
            Source(
                type="date_sequence",
                start="2024-01-01",
                end="2024-12-31",
                column="date",
                step="hour",
            )

    def test_int_sequence_step_zero_raises(self):
        """int_sequence with step=0 is rejected."""
        with pytest.raises(ValidationError, match="step"):
            Source(type="int_sequence", start=1, end=100, column="id", step=0)

    def test_int_sequence_negative_step_raises(self):
        """int_sequence with a negative step is rejected."""
        with pytest.raises(ValidationError, match="step"):
            Source(type="int_sequence", start=1, end=100, column="id", step=-1)

    def test_int_sequence_string_step_raises(self):
        """int_sequence with a string step is rejected."""
        with pytest.raises(ValidationError, match="step"):
            Source(type="int_sequence", start=1, end=100, column="id", step="2")
        with pytest.raises(ValidationError, match="step"):
            Source(type="int_sequence", start=1, end=100, column="id", step="month")

    def test_existing_delta_source_unaffected(self):
        """Existing delta source still parses correctly after adding generated type fields."""
        s = Source(type="delta", alias="raw.customers")
        assert s.type == "delta"
        assert s.start is None
        assert s.end is None
        assert s.column is None
        assert s.step is None

    def test_existing_csv_source_unaffected(self):
        """Existing CSV source still parses correctly after adding generated type fields."""
        s = Source(type="csv", path="/data/file.csv")
        assert s.type == "csv"
        assert s.start is None

    def test_existing_json_source_unaffected(self):
        """Existing JSON source still parses correctly after adding generated type fields."""
        s = Source(type="json", path="/data/events/")
        assert s.type == "json"
