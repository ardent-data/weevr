"""Tests for Export domain model."""

import pytest
from pydantic import ValidationError

from weevr.model.export import Export


class TestExportConstruction:
    """Test Export model construction and defaults."""

    def test_minimal_parquet(self):
        """Parquet export with path constructs successfully."""
        e = Export(name="archive", type="parquet", path="/data/archive")
        assert e.name == "archive"
        assert e.type == "parquet"
        assert e.path == "/data/archive"
        assert e.alias is None
        assert e.mode == "overwrite"
        assert e.on_failure == "warn"
        assert e.enabled is True

    def test_delta_with_alias(self):
        """Delta export with alias constructs successfully."""
        e = Export(name="compliance", type="delta", alias="schema.table")
        assert e.alias == "schema.table"
        assert e.path is None

    def test_delta_with_path(self):
        """Delta export with path (instead of alias) constructs successfully."""
        e = Export(name="backup", type="delta", path="/delta/backup")
        assert e.path == "/delta/backup"

    def test_csv_with_options(self):
        """CSV export with format-specific options."""
        e = Export(
            name="csv_out",
            type="csv",
            path="/exports/csv",
            options={"header": "true", "delimiter": "|"},
        )
        assert e.options == {"header": "true", "delimiter": "|"}

    def test_json_export(self):
        """JSON format export constructs successfully."""
        e = Export(name="json_feed", type="json", path="/exports/json")
        assert e.type == "json"

    def test_orc_export(self):
        """ORC format export constructs successfully."""
        e = Export(name="orc_out", type="orc", path="/exports/orc")
        assert e.type == "orc"

    def test_partition_by(self):
        """Export with partition_by list."""
        e = Export(
            name="partitioned",
            type="parquet",
            path="/data/part",
            partition_by=["region", "date"],
        )
        assert e.partition_by == ["region", "date"]

    def test_description_field(self):
        """Optional description is stored."""
        e = Export(
            name="daily_archive",
            type="parquet",
            path="/archive",
            description="Daily compliance archive",
        )
        assert e.description == "Daily compliance archive"

    def test_on_failure_abort(self):
        """Export with on_failure=abort."""
        e = Export(name="critical", type="delta", alias="db.critical", on_failure="abort")
        assert e.on_failure == "abort"


class TestExportValidation:
    """Test Export model validation constraints."""

    def test_alias_requires_delta_type(self):
        """Alias on non-delta type raises validation error."""
        with pytest.raises(ValidationError, match="alias is only valid for delta"):
            Export(name="bad", type="parquet", alias="schema.table")

    def test_path_and_alias_mutually_exclusive(self):
        """Setting both path and alias raises validation error."""
        with pytest.raises(ValidationError, match="exactly one of"):
            Export(name="both", type="delta", path="/data", alias="schema.table")

    def test_neither_path_nor_alias_when_enabled(self):
        """Omitting both path and alias when enabled raises validation error."""
        with pytest.raises(ValidationError, match="exactly one of"):
            Export(name="neither", type="parquet")

    def test_disabled_bypasses_path_alias_check(self):
        """enabled=False allows omitting both path and alias."""
        e = Export(name="suppressed", type="parquet", enabled=False)
        assert e.enabled is False
        assert e.path is None
        assert e.alias is None

    def test_invalid_name_rejected(self):
        """Non-identifier name raises validation error."""
        with pytest.raises(ValidationError, match="not a valid identifier"):
            Export(name="invalid-name", type="parquet", path="/data")

    def test_name_with_spaces_rejected(self):
        """Name with spaces raises validation error."""
        with pytest.raises(ValidationError, match="not a valid identifier"):
            Export(name="has spaces", type="parquet", path="/data")

    def test_invalid_type_rejected(self):
        """Unsupported type value raises validation error."""
        with pytest.raises(ValidationError):
            Export(name="bad_type", type="avro", path="/data")  # type: ignore[arg-type]

    def test_invalid_on_failure_rejected(self):
        """Unsupported on_failure value raises validation error."""
        with pytest.raises(ValidationError):
            Export(name="bad_fail", type="parquet", path="/data", on_failure="skip")  # type: ignore[arg-type]

    def test_invalid_mode_rejected(self):
        """Unsupported mode value raises validation error."""
        with pytest.raises(ValidationError):
            Export(name="bad_mode", type="parquet", path="/data", mode="append")  # type: ignore[arg-type]


class TestThreadWithExports:
    """Test that Thread model accepts exports."""

    def test_thread_with_exports(self):
        """Thread with exports list constructs and round-trips."""
        from weevr.model.thread import Thread

        exports = [
            Export(name="archive", type="parquet", path="/archive"),
            Export(name="copy", type="delta", alias="db.copy"),
        ]
        t = Thread.model_validate(
            {
                "config_version": "1.0",
                "sources": {"src": {"type": "delta", "alias": "db.source"}},
                "target": {"alias": "db.target"},
                "exports": [e.model_dump() for e in exports],
            }
        )
        assert t.exports is not None
        assert len(t.exports) == 2
        assert t.exports[0].name == "archive"
        assert t.exports[1].name == "copy"

    def test_thread_without_exports(self):
        """Thread without exports defaults to None."""
        from weevr.model.thread import Thread

        t = Thread.model_validate(
            {
                "config_version": "1.0",
                "sources": {"src": {"type": "delta", "alias": "db.source"}},
                "target": {"alias": "db.target"},
            }
        )
        assert t.exports is None


class TestExportImmutability:
    """Test that Export is frozen."""

    def test_mutation_raises(self):
        """Assigning to a field after construction raises ValidationError."""
        e = Export(name="frozen", type="parquet", path="/data")
        with pytest.raises(ValidationError):
            e.name = "changed"  # type: ignore[misc]

    def test_model_dump_roundtrip(self):
        """Export survives dump/validate roundtrip."""
        original = Export(
            name="roundtrip",
            type="csv",
            path="/exports/csv",
            partition_by=["date"],
            options={"header": "true"},
        )
        data = original.model_dump()
        restored = Export.model_validate(data)
        assert restored == original
