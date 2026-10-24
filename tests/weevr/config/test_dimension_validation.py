"""Tests for dimension write/keys override validation."""

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from pyspark.sql import SparkSession

from weevr.config import load_config, load_resolved
from weevr.config.validation import validate_dimension_overrides
from weevr.context import Context
from weevr.errors import ConfigError

from .conftest import MakeProject


class TestValidateDimensionOverrides:
    """Test validate_dimension_overrides diagnostics."""

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _base_config(self) -> dict:
        """Minimal thread config with a dimension block."""
        return {
            "sources": {"src": {"type": "delta", "alias": "t"}},
            "target": {
                "alias": "dim_out",
                "dimension": {
                    "business_key": ["customer_id"],
                    "surrogate_key": "customer_sk",
                    "change_detection": {"columns": ["name", "email"]},
                },
            },
        }

    # ------------------------------------------------------------------
    # write.match_keys
    # ------------------------------------------------------------------

    def test_dimension_with_write_match_keys_errors(self):
        """dimension + write.match_keys must raise an error."""
        config = self._base_config()
        config["write"] = {"match_keys": ["customer_id"], "mode": "merge"}
        diags = validate_dimension_overrides(config)
        assert any("ERROR" in d and "match_keys" in d for d in diags)

    # ------------------------------------------------------------------
    # write.on_match
    # ------------------------------------------------------------------

    def test_dimension_with_write_on_match_errors(self):
        """dimension + write.on_match must raise an error."""
        config = self._base_config()
        config["write"] = {"on_match": "update", "mode": "merge"}
        diags = validate_dimension_overrides(config)
        assert any("ERROR" in d and "on_match" in d for d in diags)

    # ------------------------------------------------------------------
    # write.mode
    # ------------------------------------------------------------------

    def test_dimension_with_write_mode_overwrite_errors(self):
        """dimension + write.mode=overwrite must raise an error."""
        config = self._base_config()
        config["write"] = {"mode": "overwrite"}
        diags = validate_dimension_overrides(config)
        assert any("ERROR" in d and "mode" in d for d in diags)

    def test_dimension_with_write_mode_merge_allowed(self):
        """dimension + write.mode=merge is valid (no error)."""
        config = self._base_config()
        config["write"] = {"mode": "merge"}
        diags = validate_dimension_overrides(config)
        assert not any("ERROR" in d for d in diags)

    def test_dimension_without_write_mode_allowed(self):
        """dimension without write.mode set is valid (engine defaults to merge)."""
        config = self._base_config()
        config["write"] = {"on_no_match_source": "soft_delete"}
        diags = validate_dimension_overrides(config)
        assert not any("ERROR" in d for d in diags)

    # ------------------------------------------------------------------
    # write.on_no_match_source and write.on_no_match_target
    # ------------------------------------------------------------------

    def test_dimension_with_on_no_match_source_soft_delete_allowed(self):
        """dimension + write.on_no_match_source=soft_delete is valid."""
        config = self._base_config()
        config["write"] = {"on_no_match_source": "soft_delete", "mode": "merge"}
        diags = validate_dimension_overrides(config)
        assert not any("ERROR" in d for d in diags)

    def test_dimension_with_on_no_match_target_allowed(self):
        """dimension + write.on_no_match_target is valid."""
        config = self._base_config()
        config["write"] = {"on_no_match_target": "insert", "mode": "merge"}
        diags = validate_dimension_overrides(config)
        assert not any("ERROR" in d for d in diags)

    # ------------------------------------------------------------------
    # keys.*
    # ------------------------------------------------------------------

    def test_dimension_with_keys_business_key_errors(self):
        """dimension + keys.business_key must raise an error."""
        config = self._base_config()
        config["keys"] = {"business_key": ["customer_id"]}
        diags = validate_dimension_overrides(config)
        assert any("ERROR" in d and "business_key" in d for d in diags)

    def test_dimension_with_keys_surrogate_key_errors(self):
        """dimension + keys.surrogate_key must raise an error."""
        config = self._base_config()
        config["keys"] = {"surrogate_key": "customer_sk"}
        diags = validate_dimension_overrides(config)
        assert any("ERROR" in d and "surrogate_key" in d for d in diags)

    def test_dimension_with_keys_change_detection_errors(self):
        """dimension + keys.change_detection must raise an error."""
        config = self._base_config()
        config["keys"] = {"change_detection": {"columns": ["name"]}}
        diags = validate_dimension_overrides(config)
        assert any("ERROR" in d and "change_detection" in d for d in diags)

    # ------------------------------------------------------------------
    # No regression: no dimension block
    # ------------------------------------------------------------------

    def test_no_dimension_write_fields_allowed(self):
        """Without dimension block, any write/keys fields are allowed."""
        config = {
            "sources": {"src": {"type": "delta", "alias": "t"}},
            "target": {"alias": "out"},
            "write": {
                "mode": "merge",
                "match_keys": ["id"],
                "on_match": "update",
            },
            "keys": {
                "business_key": ["id"],
                "surrogate_key": "sk",
                "change_detection": {"columns": ["col1"]},
            },
        }
        diags = validate_dimension_overrides(config)
        assert len(diags) == 0

    # ------------------------------------------------------------------
    # dimension present but no write / no keys
    # ------------------------------------------------------------------

    def test_dimension_without_write_no_error(self):
        """dimension without any write block produces no error."""
        config = self._base_config()
        diags = validate_dimension_overrides(config)
        assert len(diags) == 0

    def test_dimension_without_keys_no_error(self):
        """dimension without any keys block produces no error."""
        config = self._base_config()
        config["write"] = {"mode": "merge"}
        diags = validate_dimension_overrides(config)
        assert len(diags) == 0


# ---------------------------------------------------------------------------
# Enforced on every load
# ---------------------------------------------------------------------------

_DIMENSION = {
    "business_key": ["customer_id"],
    "surrogate_key": {"name": "customer_sk", "columns": ["customer_id"]},
}


def _dim_thread(**extra: object) -> dict:
    return {
        "config_version": "1.0",
        "sources": {"src": {"type": "delta", "alias": "raw.customers"}},
        "target": {"alias": "gold.dim_customer", "dimension": _DIMENSION},
        **extra,
    }


def _write(path: Path, body: dict) -> Path:
    path.write_text(json.dumps(body))
    return path


class TestDimensionRulesOnLoad:
    """Forbidden write/keys fields on a dimension thread fail the load."""

    @pytest.mark.parametrize(
        ("extra", "field"),
        [
            ({"write": {"match_keys": ["customer_id"]}}, "write.match_keys"),
            ({"write": {"on_match": "ignore"}}, "write.on_match"),
            ({"write": {"mode": "overwrite"}}, "write.mode"),
            ({"keys": {"business_key": ["customer_id"]}}, "keys.business_key"),
            (
                {"keys": {"surrogate_key": {"name": "sk", "columns": ["customer_id"]}}},
                "keys.surrogate_key",
            ),
            (
                {"keys": {"change_detection": {"name": "h", "columns": ["customer_id"]}}},
                "keys.change_detection",
            ),
        ],
    )
    def test_standalone_thread_rejected(self, tmp_path: Path, extra: dict, field: str) -> None:
        cfg = _write(tmp_path / "dim.thread", _dim_thread(**extra))
        with pytest.raises(ConfigError, match=field.replace(".", r"\.")) as exc:
            load_config(cfg)
        assert exc.value.file_path == str(cfg)

    def test_clean_dimension_thread_loads(self, tmp_path: Path) -> None:
        cfg = _write(tmp_path / "dim.thread", _dim_thread())
        assert load_config(cfg).target.dimension is not None

    def test_no_match_overrides_still_allowed(self, tmp_path: Path) -> None:
        extra = {
            "write": {"on_no_match_source": "soft_delete", "soft_delete_column": "_is_deleted"}
        }
        cfg = _write(tmp_path / "dim.thread", _dim_thread(**extra))
        assert load_config(cfg).write.on_no_match_source == "soft_delete"

    @pytest.mark.parametrize("config", ["w.weave", "l.loom"])
    def test_child_thread_rejected(self, make_project: MakeProject, config: str) -> None:
        project = make_project(_dim_thread(write={"match_keys": ["id"]}), thread_file="dim.thread")
        with pytest.raises(ConfigError, match=r"write\.match_keys") as exc:
            load_resolved(project / config, project_root=project)
        assert exc.value.file_path == str(project / "dim.thread")

    def test_inherited_defaults_are_not_the_threads_own(self, make_project: MakeProject) -> None:
        project = make_project(
            _dim_thread(),
            thread_file="dim.thread",
            weave={"defaults": {"write": {"mode": "overwrite", "match_keys": ["id"]}}},
        )
        loaded = load_resolved(project / "w.weave", project_root=project)
        assert loaded.resolved_threads["w"]["dim"].target.dimension is not None

    def test_template_entry_rejected(self, make_project: MakeProject) -> None:
        thread = _dim_thread(
            params={"region": {"type": "string"}},
            write={"match_keys": ["customer_id"]},
        )
        entry = {"ref": "dim.thread", "as": "dim_east", "params": {"region": "east"}}
        project = make_project(thread, thread_file="dim.thread", weave={"threads": [entry]})
        with pytest.raises(ConfigError, match=r"write\.match_keys"):
            load_resolved(project / "w.weave", project_root=project)

    def test_inline_thread_in_inline_weave_rejected(self, make_project: MakeProject) -> None:
        inline_thread = {"name": "dim", **_dim_thread(write={"mode": "overwrite"})}
        inline_weave = {"name": "inl", "config_version": "1.0", "threads": [inline_thread]}
        project = make_project(loom={"weaves": [inline_weave]})
        with pytest.raises(ConfigError, match=r"write\.mode") as exc:
            load_resolved(project / "l.loom", project_root=project)
        assert exc.value.file_path == str(project / "l.loom")

    def test_context_validate_reports_it(self, make_project: MakeProject) -> None:
        project = make_project(_dim_thread(keys={"business_key": ["id"]}), thread_file="dim.thread")
        ctx = Context(spark=MagicMock(spec=SparkSession), project=str(project))
        with pytest.raises(ConfigError, match=r"keys\.business_key"):
            ctx.run("w.weave", mode="validate")
