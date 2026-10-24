"""Tests for resolve step lookup name validation."""

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

from .conftest import MakeProject


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

    @pytest.mark.parametrize("key", ["with", "with_"])
    def test_with_sub_pipeline_steps_checked(self, key: str) -> None:
        config = {key: {"w": {"from": "main", "steps": [self._resolve()]}}}
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

    @pytest.fixture
    def project(self, make_project: MakeProject) -> Any:
        """``make_project`` with weave- and loom-level lookups as keywords."""

        def build(
            thread: dict, weave_lookups: dict | None = None, loom_lookups: dict | None = None
        ) -> Path:
            return make_project(
                thread,
                weave={"lookups": weave_lookups} if weave_lookups else None,
                loom={"lookups": loom_lookups} if loom_lookups else None,
            )

        return build

    def test_standalone_undefined_rejected(self, project: Any) -> None:
        proj = project(_thread([{"resolve": _RESOLVE}]))
        with pytest.raises(ConfigError, match="undefined lookup 'dim_codes'") as exc:
            load_config(proj / "fact.thread")
        assert exc.value.file_path == str(proj / "fact.thread")

    @pytest.mark.parametrize(
        "extra",
        [
            {"lookups": {"dim_codes": _LOOKUP}},
            {"lookups": {"other": _LOOKUP}, "sources": {"dim_codes": {"lookup": "other"}}},
        ],
        ids=["own-lookup", "bound-alias"],
    )
    def test_standalone_visible_loads(self, project: Any, extra: dict) -> None:
        proj = project(_thread([{"resolve": _RESOLVE}], **extra))
        load_config(proj / "fact.thread")

    def test_weave_only_lookup_not_visible_standalone(self, project: Any) -> None:
        proj = project(_thread([{"resolve": _RESOLVE}]), weave_lookups={"dim_codes": _LOOKUP})
        with pytest.raises(ConfigError, match="undefined lookup 'dim_codes'"):
            load_config(proj / "fact.thread")

    @pytest.mark.parametrize(
        ("config", "level"), [("w.weave", "weave"), ("l.loom", "loom"), ("l.loom", "weave")]
    )
    def test_weave_and_loom_lookups_visible(self, project: Any, config: str, level: str) -> None:
        lookups = {"dim_codes": _LOOKUP}
        proj = project(
            _thread([{"resolve": _RESOLVE}]),
            weave_lookups=lookups if level == "weave" else None,
            loom_lookups=lookups if level == "loom" else None,
        )
        load_resolved(proj / config, project_root=proj)

    @pytest.mark.parametrize("config", ["w.weave", "l.loom"])
    def test_undefined_in_weave_or_loom_rejected(self, project: Any, config: str) -> None:
        proj = project(
            _thread([{"resolve": {**_RESOLVE, "lookup": "nope"}}]),
            weave_lookups={"dim_codes": _LOOKUP},
        )
        with pytest.raises(ConfigError, match="undefined lookup 'nope'") as exc:
            load_resolved(proj / config, project_root=proj)
        assert exc.value.file_path == str(proj / "fact.thread")
        assert "dim_codes" in str(exc.value)

    def test_batch_undefined_rejected_even_with_warn(self, project: Any) -> None:
        batch = {"on_failure": "warn", "batch": [{**_RESOLVE, "lookup": "nope"}]}
        proj = project(_thread([{"resolve": batch}]))
        with pytest.raises(ConfigError, match="undefined lookup 'nope'"):
            load_resolved(proj / "w.weave", project_root=proj)

    def test_single_warn_loads_with_a_warning(
        self, project: Any, caplog: pytest.LogCaptureFixture
    ) -> None:
        step = {**_RESOLVE, "lookup": "nope", "on_failure": "warn"}
        proj = project(_thread([{"resolve": step}]))
        loaded = load_resolved(proj / "w.weave", project_root=proj)
        assert any("nope" in w and w.startswith("WARN:") for w in loaded.warnings)
        with caplog.at_level(logging.WARNING, logger="weevr"):
            load_config(proj / "w.weave", project_root=proj)
        assert any("nope" in r.getMessage() for r in caplog.records)

    def test_foreach_expanded_steps_checked(self, project: Any) -> None:
        foreach = {
            "foreach": {
                "values": ["dim_codes", "nope"],
                "as": "lk",
                "steps": [{"resolve": {**_RESOLVE, "name": "{lk}_sk", "lookup": "{lk}"}}],
            }
        }
        proj = project(_thread([foreach]), weave_lookups={"dim_codes": _LOOKUP})
        with pytest.raises(ConfigError, match="undefined lookup 'nope'"):
            load_resolved(proj / "w.weave", project_root=proj)

    def test_context_validate_reports_it(self, project: Any) -> None:
        proj = project(_thread([{"resolve": {**_RESOLVE, "lookup": "nope"}}]))
        ctx = Context(spark=MagicMock(spec=SparkSession), project=str(proj))
        with pytest.raises(ConfigError, match="undefined lookup 'nope'"):
            ctx.run("w.weave", mode="validate")


class TestResolveLookupsInlineAndWith:
    """Inline loom weaves/threads and with: sub-pipelines, checked on the full load."""

    def test_inline_thread_sees_loom_lookups(self, make_project: MakeProject) -> None:
        inline_thread = {"name": "fact", **_thread([{"resolve": _RESOLVE}])}
        inline_weave = {"name": "inl", "config_version": "1.0", "threads": [inline_thread]}
        proj = make_project(loom={"weaves": [inline_weave], "lookups": {"dim_codes": _LOOKUP}})
        load_resolved(proj / "l.loom", project_root=proj)

    def test_inline_thread_undefined_attributed_to_loom(self, make_project: MakeProject) -> None:
        inline_thread = {"name": "fact", **_thread([{"resolve": {**_RESOLVE, "lookup": "nope"}}])}
        inline_weave = {"name": "inl", "config_version": "1.0", "threads": [inline_thread]}
        proj = make_project(loom={"weaves": [inline_weave]})
        with pytest.raises(ConfigError, match="undefined lookup 'nope'") as exc:
            load_resolved(proj / "l.loom", project_root=proj)
        assert exc.value.file_path == str(proj / "l.loom")

    def test_with_sub_pipeline_undefined_rejected(self, make_project: MakeProject) -> None:
        thread = _thread(
            [],
            **{
                "with": {
                    "keyed": {
                        "from": "main",
                        "steps": [{"resolve": {**_RESOLVE, "lookup": "nope"}}],
                    }
                }
            },
        )
        proj = make_project(thread, weave={"lookups": {"dim_codes": _LOOKUP}})
        with pytest.raises(ConfigError, match="undefined lookup 'nope'"):
            load_resolved(proj / "w.weave", project_root=proj)

    def test_with_sub_pipeline_undefined_rejected_standalone(
        self, make_project: MakeProject
    ) -> None:
        undefined = {"resolve": {**_RESOLVE, "lookup": "nope"}}
        thread = _thread([], **{"with": {"keyed": {"from": "main", "steps": [undefined]}}})
        proj = make_project(thread)
        with pytest.raises(ConfigError, match="undefined lookup 'nope'"):
            load_config(proj / "fact.thread")
