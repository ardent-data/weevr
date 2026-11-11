"""A rename step naming a column set no visible scope defines is caught at load."""

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from pyspark.sql import SparkSession

from weevr.config import load_config, load_resolved
from weevr.config.validation import validate_rename_column_sets
from weevr.context import Context
from weevr.errors import ConfigError

from .conftest import MakeProject

_SET = {"param": "cs_map"}


def _thread(steps: list[dict], **extra: Any) -> dict[str, Any]:
    return {
        "config_version": "1.0",
        "sources": {"main": {"type": "delta", "alias": "raw.sales"}},
        "steps": steps,
        "target": {"alias": "gold.fact_sales"},
        **extra,
    }


def _rename(name: str) -> dict[str, Any]:
    return {"rename": {"column_set": name}}


class TestValidateRenameColumnSets:
    """Unit checks over a resolved thread dict."""

    def test_defined_set_passes(self) -> None:
        thread = _thread([_rename("std")])
        assert validate_rename_column_sets(thread, available_column_sets={"std"}) == []

    def test_undefined_set_is_an_error(self) -> None:
        thread = _thread([{"filter": {"expr": "x > 0"}}, _rename("nope")])
        (message,) = validate_rename_column_sets(thread, available_column_sets={"std", "alt"})
        assert message.startswith("ERROR:")
        assert "step 2" in message
        assert "'nope'" in message
        assert "['alt', 'std']" in message

    def test_no_sets_defined_says_none(self) -> None:
        (message,) = validate_rename_column_sets(
            _thread([_rename("nope")]), available_column_sets=set()
        )
        assert "(none)" in message

    def test_rename_without_column_set_is_ignored(self) -> None:
        thread = _thread([{"rename": {"columns": {"a": "b"}}}])
        assert validate_rename_column_sets(thread, available_column_sets=set()) == []

    @pytest.mark.parametrize("key", ["with", "with_"])
    def test_with_sub_pipeline_steps_checked(self, key: str) -> None:
        sub = {"orders": {"from": "main", "steps": [_rename("nope")]}}
        (message,) = validate_rename_column_sets(
            _thread([], **{key: sub}), available_column_sets=set()
        )
        assert "with.orders step 1" in message

    def test_standalone_warns(self) -> None:
        (message,) = validate_rename_column_sets(
            _thread([_rename("nope")]), available_column_sets=set(), standalone=True
        )
        assert message.startswith("WARN:")
        assert "'nope'" in message


class TestColumnSetReferencesOnLoad:
    """Load paths: weave and loom fail, a thread loaded alone warns."""

    @pytest.mark.parametrize("entry", ["w.weave", "l.loom"])
    def test_undefined_fails_through_a_parent(self, make_project: MakeProject, entry: str):
        project = make_project(_thread([_rename("nope")]), weave={"column_sets": {"std": _SET}})
        with pytest.raises(ConfigError, match="undefined column set 'nope'") as exc:
            load_config(project / entry, project_root=project)
        assert exc.value.file_path == str(project / "fact.thread")
        assert "thread 'fact'" in str(exc.value)
        assert "['std']" in str(exc.value)

    def test_undefined_fails_in_load_resolved(self, make_project: MakeProject):
        project = make_project(_thread([_rename("nope")]))
        with pytest.raises(ConfigError, match="undefined column set 'nope'"):
            load_resolved(project / "w.weave", project_root=project)

    @pytest.mark.parametrize("level", ["thread", "weave", "loom"])
    def test_defined_at_any_level_passes(self, make_project: MakeProject, level: str):
        sets = {"column_sets": {"std": _SET}}
        project = make_project(
            _thread([_rename("std")], **(sets if level == "thread" else {})),
            weave=sets if level == "weave" else None,
            loom=sets if level == "loom" else None,
        )
        loaded = load_resolved(project / "l.loom", project_root=project)
        assert not [w for w in loaded.warnings if "std" in w]

    def test_loom_set_not_visible_to_weave_loaded_alone(self, make_project: MakeProject):
        project = make_project(_thread([_rename("std")]), loom={"column_sets": {"std": _SET}})
        load_resolved(project / "l.loom", project_root=project)
        with pytest.raises(ConfigError, match="undefined column set 'std'"):
            load_resolved(project / "w.weave", project_root=project)

    def test_loaded_alone_warns_and_loads(self, make_project: MakeProject):
        project = make_project(_thread([_rename("std")]))
        loaded = load_resolved(project / "fact.thread", project_root=project)
        assert any("'std'" in w and w.startswith("WARN") for w in loaded.warnings)

    def test_loaded_alone_with_its_own_set_stays_quiet(self, make_project: MakeProject):
        project = make_project(_thread([_rename("std")], column_sets={"std": _SET}))
        loaded = load_resolved(project / "fact.thread", project_root=project)
        assert not [w for w in loaded.warnings if "std" in w]

    @pytest.mark.parametrize("entry", ["w.weave", "l.loom"])
    def test_with_sub_pipeline_undefined_fails_through_a_parent(
        self, make_project: MakeProject, entry: str
    ):
        thread = _thread([], **{"with": {"orders": {"from": "main", "steps": [_rename("nope")]}}})
        project = make_project(thread)
        with pytest.raises(ConfigError, match="rename with.orders step 1") as exc:
            load_resolved(project / entry, project_root=project)
        assert "undefined column set 'nope'" in str(exc.value)

    def test_foreach_expanded_steps_checked(self, make_project: MakeProject):
        foreach = {
            "foreach": {
                "values": ["std", "nope"],
                "as": "cs",
                "steps": [{"rename": {"column_set": "{cs}"}}],
            }
        }
        project = make_project(_thread([foreach]), weave={"column_sets": {"std": _SET}})
        with pytest.raises(ConfigError, match="undefined column set 'nope'"):
            load_resolved(project / "w.weave", project_root=project)


class TestStandaloneThreadRun:
    """A thread run on its own only sees its own sets, so a missing one fails the run."""

    @staticmethod
    def _ctx(project: Path) -> Context:
        return Context(spark=MagicMock(spec=SparkSession), project=str(project))

    @pytest.mark.parametrize("mode", ["execute", "validate", "plan", "preview"])
    def test_run_fails_before_any_mode_runs(self, make_project: MakeProject, mode: str):
        # The weave defines the set, but the thread is run on its own
        project = make_project(_thread([_rename("std")]), weave={"column_sets": {"std": _SET}})
        with pytest.raises(ConfigError, match="undefined column set 'std'") as exc:
            self._ctx(project).run("fact.thread", mode=mode)
        assert exc.value.file_path == str(project / "fact.thread")
        assert "thread 'fact'" in str(exc.value)

    def test_with_sub_pipeline_checked(self, make_project: MakeProject):
        thread = _thread([], **{"with": {"orders": {"from": "main", "steps": [_rename("nope")]}}})
        project = make_project(thread)
        with pytest.raises(ConfigError, match="rename with.orders step 1"):
            self._ctx(project).run("fact.thread", mode="validate")

    def test_own_set_runs(self, make_project: MakeProject):
        project = make_project(_thread([_rename("std")], column_sets={"std": _SET}))
        result = self._ctx(project).run("fact.thread", mode="plan")
        assert result.status == "success"

    def test_loading_alone_still_only_warns(self, make_project: MakeProject):
        project = make_project(_thread([_rename("std")]))
        loaded = self._ctx(project).load("fact.thread")
        assert loaded.config_type == "thread"

    def test_weave_run_is_unaffected(self, make_project: MakeProject):
        project = make_project(_thread([_rename("std")]), weave={"column_sets": {"std": _SET}})
        result = self._ctx(project).run("w.weave", mode="plan")
        assert result.status == "success"


@pytest.mark.spark
class TestColumnSetReferencesThroughContext:
    """An undefined set stops the run before anything is read or written."""

    def test_execute_fails_at_load_and_writes_nothing(
        self, spark: SparkSession, make_project: MakeProject, tmp_path: Path
    ) -> None:
        src = str(tmp_path / "src")
        tgt = tmp_path / "tgt"
        spark.createDataFrame([{"a": 1}]).write.format("delta").save(src)
        thread = {
            "config_version": "1.0",
            "sources": {"main": {"type": "delta", "alias": src}},
            "steps": [_rename("nope")],
            "target": {"path": str(tgt)},
            "write": {"mode": "overwrite"},
        }
        project = make_project(thread)
        ctx = Context(spark=spark, project=str(project))
        with pytest.raises(ConfigError, match="undefined column set 'nope'"):
            ctx.run("w.weave")
        assert not tgt.exists()
