"""Tests for the dimension merge builder."""

import pytest
from pyspark.sql import SparkSession

from weevr.model.dimension import DimensionConfig
from weevr.model.write import WriteConfig
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
