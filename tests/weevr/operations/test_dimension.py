"""Tests for the dimension merge builder."""

import logging

import pytest
from pyspark.sql import SparkSession

from weevr.model.dimension import DimensionConfig
from weevr.model.write import WriteConfig
from weevr.operations import hashing
from weevr.operations.dimension import DimensionMergeBuilder


def _make_dim_config(**overrides):  # type: ignore[no-untyped-def]
    base = {
        "business_key": ["customer_id"],
        "surrogate_key": {"name": "_sk", "columns": ["customer_id"]},
    }
    return DimensionConfig(**{**base, **overrides})


class TestBuildWriteConfig:
    """Tests for DimensionMergeBuilder.build_write_config."""

    def test_generates_merge_mode(self) -> None:
        dim = _make_dim_config()
        builder = DimensionMergeBuilder(dim)
        wc = builder.build_write_config()
        assert wc.mode == "merge"

    def test_match_keys_from_business_key(self) -> None:
        dim = _make_dim_config(business_key=["cust_id", "region"])
        builder = DimensionMergeBuilder(dim)
        wc = builder.build_write_config()
        assert wc.match_keys == ["cust_id", "region"]

    def test_default_on_no_match_source_is_ignore(self) -> None:
        dim = _make_dim_config()
        builder = DimensionMergeBuilder(dim)
        wc = builder.build_write_config()
        assert wc.on_no_match_source == "ignore"

    def test_overrides_applied(self) -> None:
        dim = _make_dim_config()
        override = WriteConfig(
            mode="merge",
            match_keys=["ignored"],
            on_no_match_source="soft_delete",
            soft_delete_column="_deleted",
        )
        builder = DimensionMergeBuilder(dim, write_overrides=override)
        wc = builder.build_write_config()
        assert wc.on_no_match_source == "soft_delete"
        assert wc.soft_delete_column == "_deleted"
        # match_keys always from BK, not override
        assert wc.match_keys == ["customer_id"]

    def test_on_match_always_update(self) -> None:
        dim = _make_dim_config()
        builder = DimensionMergeBuilder(dim)
        wc = builder.build_write_config()
        assert wc.on_match == "update"


class TestInjectScdColumns:
    """Tests for DimensionMergeBuilder.inject_scd_columns."""

    @pytest.mark.spark
    def test_no_injection_without_track_history(self, spark: SparkSession) -> None:
        dim = _make_dim_config(track_history=False)
        builder = DimensionMergeBuilder(dim)
        df = spark.createDataFrame([{"customer_id": "C1"}])
        result = builder.inject_scd_columns(df, "2026-01-01")
        assert "_valid_from" not in result.columns

    @pytest.mark.spark
    def test_injects_scd_columns_with_track_history(self, spark: SparkSession) -> None:
        dim = _make_dim_config(
            track_history=True,
            change_detection={  # type: ignore[arg-type]
                "cd": {
                    "columns": "auto",
                    "on_change": "version",
                },
            },
        )
        builder = DimensionMergeBuilder(dim)
        df = spark.createDataFrame([{"customer_id": "C1"}])
        result = builder.inject_scd_columns(df, "2026-03-28")
        assert "_valid_from" in result.columns
        assert "_valid_to" in result.columns
        assert "_is_current" in result.columns
        row = result.collect()[0]
        assert row["_valid_from"] == "2026-03-28"
        assert row["_valid_to"] == "9999-12-31"
        assert row["_is_current"] is True

    @pytest.mark.spark
    def test_custom_scd_column_names(self, spark: SparkSession) -> None:
        dim = _make_dim_config(
            track_history=True,
            change_detection={  # type: ignore[arg-type]
                "cd": {"columns": "auto", "on_change": "version"},
            },
            columns={"valid_from": "eff_from", "valid_to": "eff_to", "is_current": "active"},  # type: ignore[arg-type]
        )
        builder = DimensionMergeBuilder(dim)
        df = spark.createDataFrame([{"customer_id": "C1"}])
        result = builder.inject_scd_columns(df, "2026-01-01")
        assert "eff_from" in result.columns
        assert "eff_to" in result.columns
        assert "active" in result.columns


class TestGetSystemMemberSkValues:
    """Tests for DimensionMergeBuilder.get_system_member_sk_values."""

    def test_empty_when_not_seeding(self) -> None:
        dim = _make_dim_config(seed_system_members=False)
        builder = DimensionMergeBuilder(dim)
        assert builder.get_system_member_sk_values() == set()

    def test_default_sk_values(self) -> None:
        dim = _make_dim_config(seed_system_members=True)
        builder = DimensionMergeBuilder(dim)
        sks = builder.get_system_member_sk_values()
        assert -1 in sks
        assert -2 in sks

    def test_custom_sk_values(self) -> None:
        dim = _make_dim_config(
            seed_system_members=True,
            system_members=[  # type: ignore[arg-type]
                {"sk": -10, "code": "custom", "label": "Custom"},
            ],
        )
        builder = DimensionMergeBuilder(dim)
        assert builder.get_system_member_sk_values() == {-10}


class TestGetScdColumnNames:
    """Tests for DimensionMergeBuilder.get_scd_column_names."""

    def test_default_names(self) -> None:
        dim = _make_dim_config()
        builder = DimensionMergeBuilder(dim)
        names = builder.get_scd_column_names()
        assert names == {"_valid_from", "_valid_to", "_is_current"}

    def test_custom_names(self) -> None:
        dim = _make_dim_config(
            columns={"valid_from": "a", "valid_to": "b", "is_current": "c"},  # type: ignore[arg-type]
        )
        builder = DimensionMergeBuilder(dim)
        assert builder.get_scd_column_names() == {"a", "b", "c"}


class TestHasVersionGroups:
    """Tests for DimensionMergeBuilder.has_version_groups."""

    def test_false_for_overwrite_only(self) -> None:
        dim = _make_dim_config()  # defaults to overwrite
        builder = DimensionMergeBuilder(dim)
        assert builder.has_version_groups() is False

    def test_true_when_version_group_present(self) -> None:
        dim = _make_dim_config(
            track_history=True,
            change_detection={  # type: ignore[arg-type]
                "cd": {"columns": "auto", "on_change": "version"},
            },
        )
        builder = DimensionMergeBuilder(dim)
        assert builder.has_version_groups() is True


class TestGetOverwriteColumns:
    """Tests for DimensionMergeBuilder.get_overwrite_columns."""

    def test_empty_when_no_overwrite_groups(self) -> None:
        dim = _make_dim_config(
            track_history=True,
            change_detection={  # type: ignore[arg-type]
                "cd": {"columns": "auto", "on_change": "version"},
            },
        )
        builder = DimensionMergeBuilder(dim)
        assert builder.get_overwrite_columns() == []

    def test_returns_overwrite_group_columns(self) -> None:
        dim = _make_dim_config(
            track_history=True,
            change_detection={  # type: ignore[arg-type]
                "ver": {"columns": ["name"], "on_change": "version"},
                "ow": {"columns": ["status", "tier"], "on_change": "overwrite"},
            },
        )
        builder = DimensionMergeBuilder(dim)
        assert sorted(builder.get_overwrite_columns()) == ["status", "tier"]


class TestResolveOverwriteColumns:
    """Tests for DimensionMergeBuilder.resolve_overwrite_columns.

    The effective overwrite set covers explicit overwrite groups, an
    overwrite group with ``columns: auto``, and unassigned payload columns.
    """

    # Frame columns as they reach the merge: payload, keys, the "tracked"
    # group hash (other group outputs are excluded by name either way),
    # SCD columns, and one audit column.
    FRAME = [
        "customer_id",
        "name",
        "email",
        "tier",
        "region",
        "_sk",
        "tracked",
        "_valid_from",
        "_valid_to",
        "_is_current",
        "_loaded_at",
    ]

    def test_explicit_overwrite_group(self) -> None:
        dim = _make_dim_config(
            track_history=True,
            change_detection={  # type: ignore[arg-type]
                "tracked": {"columns": ["email", "tier"], "on_change": "version"},
                "attrs": {"columns": ["name"], "on_change": "overwrite"},
                "fixed": {"columns": ["region"], "on_change": "static"},
            },
        )
        builder = DimensionMergeBuilder(dim, audit_columns={"_loaded_at"})
        assert builder.resolve_overwrite_columns(self.FRAME) == ["name"]

    def test_auto_overwrite_group_matches_its_hash_columns(self) -> None:
        dim = _make_dim_config(
            track_history=True,
            change_detection={  # type: ignore[arg-type]
                "tracked": {"columns": ["email", "tier"], "on_change": "version"},
                "attrs": {"columns": "auto", "on_change": "overwrite"},
            },
        )
        builder = DimensionMergeBuilder(dim, audit_columns={"_loaded_at"})
        assert builder.resolve_overwrite_columns(self.FRAME) == ["name", "region"]

    def test_unassigned_columns_without_auto_group(self) -> None:
        dim = _make_dim_config(
            track_history=True,
            change_detection={  # type: ignore[arg-type]
                "tracked": {"columns": ["email", "tier"], "on_change": "version"},
            },
        )
        builder = DimensionMergeBuilder(dim, audit_columns={"_loaded_at"})
        assert builder.resolve_overwrite_columns(self.FRAME) == ["name", "region"]

    def test_explicit_and_unassigned_combined(self) -> None:
        dim = _make_dim_config(
            track_history=True,
            change_detection={  # type: ignore[arg-type]
                "tracked": {"columns": ["email"], "on_change": "version"},
                "attrs": {"columns": ["tier"], "on_change": "overwrite"},
            },
        )
        builder = DimensionMergeBuilder(dim, audit_columns={"_loaded_at"})
        assert builder.resolve_overwrite_columns(self.FRAME) == ["tier", "name", "region"]

    def test_auto_version_group_leaves_nothing_unassigned(self) -> None:
        dim = _make_dim_config(
            track_history=True,
            change_detection={  # type: ignore[arg-type]
                "tracked": {"columns": "auto", "on_change": "version"},
                "attrs": {"columns": ["name"], "on_change": "overwrite"},
            },
        )
        builder = DimensionMergeBuilder(dim, audit_columns={"_loaded_at"})
        assert builder.resolve_overwrite_columns(self.FRAME) == ["name"]

    def test_auto_static_group_leaves_nothing_unassigned(self) -> None:
        dim = _make_dim_config(
            track_history=True,
            change_detection={  # type: ignore[arg-type]
                "tracked": {"columns": ["email"], "on_change": "version"},
                "fixed": {"columns": "auto", "on_change": "static"},
            },
        )
        builder = DimensionMergeBuilder(dim, audit_columns={"_loaded_at"})
        assert builder.resolve_overwrite_columns(self.FRAME) == []

    def test_engine_managed_columns_never_included(self) -> None:
        dim = _make_dim_config(
            track_history=True,
            change_detection={  # type: ignore[arg-type]
                "tracked": {"columns": ["email"], "on_change": "version"},
            },
            additional_keys={  # type: ignore[arg-type]
                "nk": {"name": "_nk", "columns": ["region"]},
            },
            previous_columns={"_prev_email": "email"},
        )
        frame = [*self.FRAME, "_nk", "_prev_email"]
        builder = DimensionMergeBuilder(dim, audit_columns={"_loaded_at"})
        # region is an additional-key source; _nk, _prev_email, SCD, SK,
        # BK, hashes, and audit columns are engine-managed.
        assert builder.resolve_overwrite_columns(frame) == ["name", "tier"]

    def test_audit_columns_unassigned_without_audit_names(self) -> None:
        dim = _make_dim_config(
            track_history=True,
            change_detection={  # type: ignore[arg-type]
                "tracked": {"columns": ["email", "tier"], "on_change": "version"},
            },
        )
        builder = DimensionMergeBuilder(dim)
        assert "_loaded_at" in builder.resolve_overwrite_columns(self.FRAME)

    def test_previous_columns_source_that_is_unassigned_included(self) -> None:
        dim = _make_dim_config(
            track_history=True,
            change_detection={  # type: ignore[arg-type]
                "tracked": {"columns": ["email"], "on_change": "version"},
            },
            previous_columns={"_prev_tier": "tier"},
        )
        frame = [*self.FRAME, "_prev_tier"]
        builder = DimensionMergeBuilder(dim, audit_columns={"_loaded_at"})
        cols = builder.resolve_overwrite_columns(frame)
        assert "tier" in cols
        assert "_prev_tier" not in cols

    def test_matches_unassigned_log_line(self, caplog: pytest.LogCaptureFixture) -> None:
        """The columns logged as defaulting to overwrite are the ones overwritten."""
        dim = _make_dim_config(
            track_history=True,
            change_detection={  # type: ignore[arg-type]
                "tracked": {"columns": ["email", "tier"], "on_change": "version"},
            },
        )
        payload = ["customer_id", "name", "email", "tier", "region", "_loaded_at"]
        with caplog.at_level(logging.INFO, logger=hashing.__name__):
            hashing.log_unassigned_columns(payload, dim, audit_columns={"_loaded_at"})
        logged = [r.getMessage() for r in caplog.records if "default to overwrite" in r.message]
        builder = DimensionMergeBuilder(dim, audit_columns={"_loaded_at"})
        assert logged == [
            "Unassigned columns default to overwrite: "
            + ", ".join(builder.resolve_overwrite_columns(self.FRAME))
        ]


class TestGetOverwriteHashColumns:
    """Tests for DimensionMergeBuilder.get_overwrite_hash_columns."""

    def test_overwrite_group_outputs(self) -> None:
        dim = _make_dim_config(
            track_history=True,
            change_detection={  # type: ignore[arg-type]
                "tracked": {"columns": ["email"], "on_change": "version"},
                "attrs": {"columns": ["name"], "on_change": "overwrite"},
                "more": {"name": "_h_more", "columns": "auto", "on_change": "overwrite"},
                "fixed": {"columns": ["region"], "on_change": "static"},
            },
        )
        builder = DimensionMergeBuilder(dim)
        assert builder.get_overwrite_hash_columns() == ["attrs", "_h_more"]

    def test_default_group_versions_with_history(self) -> None:
        # The default group is auto + version under track_history.
        builder = DimensionMergeBuilder(_make_dim_config(track_history=True))
        assert builder.get_overwrite_hash_columns() == []
        assert builder.resolve_overwrite_columns(["customer_id", "name", "_sk"]) == []


class TestGetStaticColumns:
    """Tests for DimensionMergeBuilder.get_static_columns."""

    def test_returns_static_group_columns(self) -> None:
        dim = _make_dim_config(
            change_detection={  # type: ignore[arg-type]
                "ow": {"columns": ["name"], "on_change": "overwrite"},
                "st": {"columns": ["created_at"], "on_change": "static"},
            },
        )
        builder = DimensionMergeBuilder(dim)
        assert builder.get_static_columns() == ["created_at"]
