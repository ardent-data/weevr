"""Tests for warp discovery logic."""

from pathlib import Path

import pytest

from weevr.config.warp import (
    build_effective_warp,
    get_drift_baseline,
    resolve_warp,
)
from weevr.errors import ConfigError
from weevr.model.warp import WarpColumn, WarpConfig

FIXTURES_DIR = Path(__file__).parent / "fixtures" / "warps"


class TestResolveWarp:
    """Test resolve_warp function."""

    def test_explicit_reference_resolves_from_thread_dir(self):
        """Explicit warp reference resolves from thread directory."""
        result = resolve_warp(
            warp_ref="dim_customer",
            thread_dir=FIXTURES_DIR,
            config_root=FIXTURES_DIR.parent,
        )
        assert result is not None
        assert isinstance(result, WarpConfig)
        assert result.columns[0].name == "customer_sk"

    def test_explicit_reference_with_extension(self):
        """Explicit reference with .warp extension resolves."""
        result = resolve_warp(
            warp_ref="dim_customer.warp",
            thread_dir=FIXTURES_DIR,
            config_root=FIXTURES_DIR.parent,
        )
        assert result is not None

    def test_explicit_reference_falls_back_to_config_root(self):
        """Explicit reference falls back to config root if not in thread dir."""
        # thread_dir that doesn't contain the warp
        nonexistent_dir = FIXTURES_DIR / "subdir"
        result = resolve_warp(
            warp_ref="dim_customer",
            thread_dir=nonexistent_dir,
            config_root=FIXTURES_DIR,
        )
        assert result is not None
        assert result.columns[0].name == "customer_sk"

    def test_explicit_reference_with_path_separator(self):
        """Explicit reference with directory resolves from config root."""
        result = resolve_warp(
            warp_ref="warps/dim_customer",
            thread_dir=FIXTURES_DIR.parent.parent,
            config_root=FIXTURES_DIR.parent,
        )
        assert result is not None

    def test_auto_discovery_by_alias(self):
        """Auto-discovery finds .warp by target alias (last dot segment)."""
        result = resolve_warp(
            warp_ref=None,
            thread_dir=FIXTURES_DIR,
            config_root=FIXTURES_DIR.parent,
            target_alias="gold.dim_customer",
        )
        assert result is not None
        assert result.columns[0].name == "customer_sk"

    def test_auto_discovery_by_table_name(self):
        """Auto-discovery finds .warp by simple table name."""
        result = resolve_warp(
            warp_ref=None,
            thread_dir=FIXTURES_DIR,
            config_root=FIXTURES_DIR.parent,
            target_alias="dim_customer",
        )
        assert result is not None

    def test_opt_out_with_false(self):
        """warp: false returns None."""
        result = resolve_warp(
            warp_ref=False,
            thread_dir=FIXTURES_DIR,
            config_root=FIXTURES_DIR.parent,
        )
        assert result is None

    def test_missing_explicit_warp_raises(self):
        """Explicit warp reference to nonexistent file raises ConfigError."""
        with pytest.raises(ConfigError, match="not found"):
            resolve_warp(
                warp_ref="nonexistent_warp",
                thread_dir=FIXTURES_DIR,
                config_root=FIXTURES_DIR.parent,
            )

    def test_no_warp_no_alias_returns_none(self):
        """No warp file and no explicit ref returns None."""
        result = resolve_warp(
            warp_ref=None,
            thread_dir=FIXTURES_DIR,
            config_root=FIXTURES_DIR.parent,
            target_alias="nonexistent_table",
        )
        assert result is None

    def test_no_warp_ref_no_alias_returns_none(self):
        """No warp ref and no alias returns None (nothing to discover)."""
        result = resolve_warp(
            warp_ref=None,
            thread_dir=FIXTURES_DIR,
            config_root=FIXTURES_DIR.parent,
        )
        assert result is None


class TestBuildEffectiveWarp:
    """Test build_effective_warp function."""

    def _make_columns(self, names: list[str]) -> list[WarpColumn]:
        return [WarpColumn(name=n, type="string") for n in names]

    def test_all_present_no_warp_only(self):
        """All warp columns present in pipeline → no warp-only."""
        warp_cols = self._make_columns(["a", "b", "c"])
        pipeline_cols = ["a", "b", "c"]
        effective = build_effective_warp(warp_cols, pipeline_cols, [])
        assert len(effective.declared) == 3
        assert len(effective.warp_only) == 0
        assert effective.engine == []

    def test_some_absent_computed_as_warp_only(self):
        """Warp columns absent from pipeline → computed as warp-only."""
        warp_cols = self._make_columns(["a", "b", "c", "d"])
        pipeline_cols = ["a", "b"]
        effective = build_effective_warp(warp_cols, pipeline_cols, [])
        assert len(effective.declared) == 4
        assert len(effective.warp_only) == 2
        assert {c.name for c in effective.warp_only} == {"c", "d"}

    def test_engine_columns_added(self):
        """Engine columns added without duplication."""
        warp_cols = self._make_columns(["a"])
        effective = build_effective_warp(warp_cols, ["a"], ["_loaded_at", "_run_id"])
        assert effective.engine == ["_loaded_at", "_run_id"]
        assert "_loaded_at" in effective.all_columns
        assert "_run_id" in effective.all_columns

    def test_all_columns_returns_union(self):
        """all_columns returns complete list."""
        warp_cols = self._make_columns(["a", "b"])
        effective = build_effective_warp(warp_cols, ["a"], ["_audit"])
        names = effective.all_columns
        assert "a" in names
        assert "b" in names
        assert "_audit" in names

    def test_engine_columns_not_duplicated(self):
        """Engine columns that are also in warp are not duplicated in engine list."""
        warp_cols = self._make_columns(["a", "_audit"])
        effective = build_effective_warp(warp_cols, ["a", "_audit"], ["_audit"])
        assert effective.engine == []


class TestGetDriftBaseline:
    """Test get_drift_baseline function."""

    def test_with_warp_returns_warp_column_names(self):
        """With warp: returns warp column names as baseline."""
        warp = WarpConfig(
            config_version="1.0",
            columns=[  # type: ignore[arg-type]
                {"name": "customer_id", "type": "bigint"},
                {"name": "customer_name", "type": "string"},
            ],
        )
        baseline = get_drift_baseline(warp=warp)
        assert baseline == ["customer_id", "customer_name"]

    def test_without_warp_returns_none(self):
        """Without warp and no Spark session: returns None."""
        baseline = get_drift_baseline(warp=None)
        assert baseline is None
