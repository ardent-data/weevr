"""Tests for resolve step lookup name validation."""

import json
import logging
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from pyspark.sql import SparkSession

from weevr.config import load_config, load_resolved
from weevr.config.validation import validate_resolve_lookups
from weevr.context import Context
from weevr.errors import ConfigError


class TestValidateResolveLookups:
    """Test validate_resolve_lookups diagnostics."""

    def test_valid_lookup_reference(self):
        """Resolve step referencing a defined lookup produces no errors."""
        config = {
            "steps": [
                {"resolve": {"name": "fk", "lookup": "dim_plant", "match": "bk", "pk": "id"}}
            ],
        }
        diags = validate_resolve_lookups(config, available_lookups={"dim_plant"})
        assert not diags

    def test_undefined_lookup_reference(self):
        """Resolve step referencing an undefined lookup produces an error."""
        config = {
            "steps": [
                {"resolve": {"name": "fk", "lookup": "dim_missing", "match": "bk", "pk": "id"}}
            ],
        }
        diags = validate_resolve_lookups(config, available_lookups={"dim_plant"})
        assert any("ERROR" in d and "dim_missing" in d for d in diags)

    def test_batch_items_validated(self):
        """Batch resolve items each have their lookups validated."""
        config = {
            "steps": [
                {
                    "resolve": {
                        "pk": "id",
                        "batch": [
                            {"name": "fk1", "lookup": "dim_a", "match": "bk1"},
                            {"name": "fk2", "lookup": "dim_missing", "match": "bk2"},
                        ],
                    }
                }
            ],
        }
        diags = validate_resolve_lookups(config, available_lookups={"dim_a", "dim_b"})
        errors = [d for d in diags if "ERROR" in d]
        assert len(errors) == 1
        assert "dim_missing" in errors[0]

    def test_no_resolve_steps_no_errors(self):
        """Config without resolve steps produces no diagnostics."""
        config = {
            "steps": [
                {"filter": {"expr": "x > 0"}},
                {"derive": {"columns": {"y": "x + 1"}}},
            ],
        }
        diags = validate_resolve_lookups(config, available_lookups=set())
        assert not diags

    def test_multiple_resolve_steps(self):
        """Multiple resolve steps are all validated."""
        config = {
            "steps": [
                {"resolve": {"name": "fk1", "lookup": "dim_a", "match": "b", "pk": "id"}},
                {"resolve": {"name": "fk2", "lookup": "dim_b", "match": "b", "pk": "id"}},
            ],
        }
        diags = validate_resolve_lookups(config, available_lookups={"dim_a"})
        assert len([d for d in diags if "ERROR" in d]) == 1
        assert any("dim_b" in d for d in diags)

    def test_empty_available_lookups(self):
        """All resolve lookups are errors when no lookups are available."""
        config = {
            "steps": [{"resolve": {"name": "fk", "lookup": "dim_x", "match": "b", "pk": "id"}}],
        }
        diags = validate_resolve_lookups(config, available_lookups=set())
        assert any("dim_x" in d for d in diags)


class TestValidatorBranches:
    """on_failure and with: handling in the validator itself."""

    @staticmethod
    def _resolve(**extra: Any) -> dict:
        return {"resolve": {"name": "fk", "lookup": "nope", "match": "bk", "pk": "id", **extra}}

    def test_single_warn_is_a_warning(self) -> None:
        diags = validate_resolve_lookups(
            {"steps": [self._resolve(on_failure="warn")]}, available_lookups=set()
        )
        assert diags and all(d.startswith("WARN:") for d in diags)

    def test_batch_errors_even_with_warn(self) -> None:
        batch = {
            "resolve": {
                "on_failure": "warn",
                "batch": [{"name": "fk", "lookup": "nope", "match": "bk", "pk": "id"}],
            }
        }
        diags = validate_resolve_lookups({"steps": [batch]}, available_lookups=set())
        assert diags and all(d.startswith("ERROR:") for d in diags)

    def test_with_sub_pipeline_steps_checked(self) -> None:
        config = {"with": {"w": {"from": "main", "steps": [self._resolve()]}}}
        diags = validate_resolve_lookups(config, available_lookups=set())
        assert any(d.startswith("ERROR:") and "nope" in d for d in diags)


_RESOLVE = {"name": "code_sk", "lookup": "dim_codes", "match": "code", "pk": "sk"}
_LOOKUP = {"source": {"type": "delta", "alias": "gold.dim_codes"}}


def _thread(steps: list[dict], **extra: Any) -> dict:
    return {
        "config_version": "1.0",
        "sources": {"main": {"type": "delta", "alias": "raw.sales"}, **extra.pop("sources", {})},
        "steps": steps,
        "target": {"alias": "gold.fact_sales"},
        **extra,
    }


class TestResolveLookupsOnLoad:
    """A resolve step naming a lookup its thread can't see is caught at load."""

    @staticmethod
    def _project(
        tmp_path: Path,
        thread: dict,
        weave_lookups: dict | None = None,
        loom_lookups: dict | None = None,
    ) -> Path:
        project = tmp_path / "p.weevr"
        project.mkdir()
        (project / "fact.thread").write_text(json.dumps(thread))
        weave: dict = {"config_version": "1.0", "threads": [{"ref": "fact.thread"}]}
        if weave_lookups:
            weave["lookups"] = weave_lookups
        (project / "w.weave").write_text(json.dumps(weave))
        loom: dict = {"config_version": "1.0", "weaves": [{"ref": "w.weave"}]}
        if loom_lookups:
            loom["lookups"] = loom_lookups
        (project / "l.loom").write_text(json.dumps(loom))
        return project

    def test_standalone_undefined_rejected(self, tmp_path: Path) -> None:
        project = self._project(tmp_path, _thread([{"resolve": _RESOLVE}]))
        with pytest.raises(ConfigError, match="undefined lookup 'dim_codes'") as exc:
            load_config(project / "fact.thread")
        assert exc.value.file_path == str(project / "fact.thread")

    @pytest.mark.parametrize(
        "extra",
        [
            {"lookups": {"dim_codes": _LOOKUP}},
            {"lookups": {"other": _LOOKUP}, "sources": {"dim_codes": {"lookup": "other"}}},
        ],
        ids=["own-lookup", "bound-alias"],
    )
    def test_standalone_visible_loads(self, tmp_path: Path, extra: dict) -> None:
        project = self._project(tmp_path, _thread([{"resolve": _RESOLVE}], **extra))
        load_config(project / "fact.thread")

    def test_weave_only_lookup_not_visible_standalone(self, tmp_path: Path) -> None:
        project = self._project(
            tmp_path, _thread([{"resolve": _RESOLVE}]), weave_lookups={"dim_codes": _LOOKUP}
        )
        with pytest.raises(ConfigError, match="undefined lookup 'dim_codes'"):
            load_config(project / "fact.thread")

    @pytest.mark.parametrize(
        ("config", "level"), [("w.weave", "weave"), ("l.loom", "loom"), ("l.loom", "weave")]
    )
    def test_weave_and_loom_lookups_visible(self, tmp_path: Path, config: str, level: str) -> None:
        lookups = {"dim_codes": _LOOKUP}
        project = self._project(
            tmp_path,
            _thread([{"resolve": _RESOLVE}]),
            weave_lookups=lookups if level == "weave" else None,
            loom_lookups=lookups if level == "loom" else None,
        )
        load_resolved(project / config, project_root=project)

    @pytest.mark.parametrize("config", ["w.weave", "l.loom"])
    def test_undefined_in_weave_or_loom_rejected(self, tmp_path: Path, config: str) -> None:
        project = self._project(
            tmp_path,
            _thread([{"resolve": {**_RESOLVE, "lookup": "nope"}}]),
            weave_lookups={"dim_codes": _LOOKUP},
        )
        with pytest.raises(ConfigError, match="undefined lookup 'nope'") as exc:
            load_resolved(project / config, project_root=project)
        assert exc.value.file_path == str(project / "fact.thread")
        assert "dim_codes" in str(exc.value)

    def test_batch_undefined_rejected_even_with_warn(self, tmp_path: Path) -> None:
        batch = {"on_failure": "warn", "batch": [{**_RESOLVE, "lookup": "nope"}]}
        project = self._project(tmp_path, _thread([{"resolve": batch}]))
        with pytest.raises(ConfigError, match="undefined lookup 'nope'"):
            load_resolved(project / "w.weave", project_root=project)

    def test_single_warn_loads_with_a_warning(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        step = {**_RESOLVE, "lookup": "nope", "on_failure": "warn"}
        project = self._project(tmp_path, _thread([{"resolve": step}]))
        loaded = load_resolved(project / "w.weave", project_root=project)
        assert any("nope" in w and w.startswith("WARN:") for w in loaded.warnings)
        with caplog.at_level(logging.WARNING, logger="weevr"):
            load_config(project / "w.weave", project_root=project)
        assert any("nope" in r.getMessage() for r in caplog.records)

    def test_foreach_expanded_steps_checked(self, tmp_path: Path) -> None:
        foreach = {
            "foreach": {
                "values": ["dim_codes", "nope"],
                "as": "lk",
                "steps": [{"resolve": {**_RESOLVE, "name": "{lk}_sk", "lookup": "{lk}"}}],
            }
        }
        project = self._project(tmp_path, _thread([foreach]), weave_lookups={"dim_codes": _LOOKUP})
        with pytest.raises(ConfigError, match="undefined lookup 'nope'"):
            load_resolved(project / "w.weave", project_root=project)

    def test_context_validate_reports_it(self, tmp_path: Path) -> None:
        project = self._project(tmp_path, _thread([{"resolve": {**_RESOLVE, "lookup": "nope"}}]))
        ctx = Context(spark=MagicMock(spec=SparkSession), project=str(project))
        with pytest.raises(ConfigError, match="undefined lookup 'nope'"):
            ctx.run("w.weave", mode="validate")
