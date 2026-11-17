"""Diagnostic codes raised by the execution planner and surfaced on plan issues."""

import pytest

from weevr.engine.planner import build_plan
from weevr.errors.exceptions import ConfigError
from weevr.model.source import Source
from weevr.model.target import Target
from weevr.model.thread import Thread
from weevr.model.weave import ThreadEntry, Weave
from weevr.result import LoadedConfig, PlanIssue


def _thread(name: str, target: str, source: str | None = None) -> Thread:
    return Thread(
        name=name,
        config_version="1.0",
        sources={"main": Source(type="delta", alias=source or f"raw.{name}")},
        target=Target(alias=target),
    )


def _raised(threads: dict[str, Thread], entries: list[ThreadEntry]) -> ConfigError:
    with pytest.raises(ConfigError) as info:
        build_plan(weave_name="w", threads=threads, thread_entries=entries)
    return info.value


class TestPlannerCodes:
    def test_cycle_is_d101_with_cycle_order(self) -> None:
        err = _raised(
            {"a": _thread("a", "out.a"), "b": _thread("b", "out.b")},
            [
                ThreadEntry(name="a", dependencies=["b"]),
                ThreadEntry(name="b", dependencies=["a"]),
            ],
        )
        diag = err.diagnostics[0]
        assert diag.code == "D101"
        assert diag.severity == "error"
        assert diag.node == "w"
        assert diag.expected is not None
        assert diag.expected[0] == diag.expected[-1]
        assert set(diag.expected) == {"a", "b"}

    def test_conflicting_writers_are_d102(self) -> None:
        err = _raised(
            {"jan": _thread("jan", "out.same"), "feb": _thread("feb", "out.same")},
            [ThreadEntry(name="jan"), ThreadEntry(name="feb")],
        )
        diag = err.diagnostics[0]
        assert diag.code == "D102"
        assert diag.severity == "error"
        assert diag.node == "w"
        assert diag.expected == ("feb", "jan")

    def test_undefined_dependency_is_d103(self) -> None:
        err = _raised(
            {"a": _thread("a", "out.a")},
            [ThreadEntry(name="a", dependencies=["ghost"])],
        )
        diag = err.diagnostics[0]
        assert diag.code == "D103"
        assert diag.severity == "error"
        assert diag.node == "a"
        assert diag.expected == ("a",)


class TestPlanIssueDiagnostic:
    def test_issue_carries_diagnostic_and_keeps_existing_fields(self) -> None:
        threads = {"a": _thread("a", "out.a"), "b": _thread("b", "out.b")}
        weave_cfg = {
            "name": "cyc",
            "config_version": "1.0",
            "threads": [
                ThreadEntry(name="a", dependencies=["b"]),
                ThreadEntry(name="b", dependencies=["a"]),
            ],
        }
        weave = Weave(**weave_cfg)
        loaded = LoadedConfig(
            model=weave, config_type="weave", config_name="cyc", threads={"cyc": threads}
        )
        (issue,) = loaded.validate_plans()
        assert issue.weave_name == "cyc"
        assert isinstance(issue.error, ConfigError)
        assert issue.message == issue.error.plain_text()
        assert issue.diagnostic is not None
        assert issue.diagnostic.code == "D101"
        assert issue.diagnostic is issue.error.diagnostics[0]

    def test_diagnostic_defaults_to_none(self) -> None:
        assert PlanIssue("w", "m", None).diagnostic is None


def test_missing_weave_issue_is_coded() -> None:
    from weevr.model import Loom
    from weevr.result import LoadedConfig

    loom = Loom.model_validate({"config_version": "1.0", "name": "l", "weaves": ["w"]})
    loaded = LoadedConfig(loom, "loom", "l", threads={}, weaves={})
    (issue,) = loaded.validate_plans()
    assert issue.error is None
    assert issue.diagnostic is not None
    assert (issue.diagnostic.code, issue.diagnostic.node) == ("D104", "w")


def test_cycle_diagnostic_names_the_weave_file(tmp_path) -> None:
    import yaml

    from weevr.config import load_resolved

    def thread(name: str) -> dict:
        return {
            "config_version": "1.0",
            "sources": {"s": {"type": "delta", "alias": f"raw.{name}"}},
            "target": {"alias": f"out.{name}"},
        }

    for name in ("a", "b"):
        (tmp_path / f"{name}.thread").write_text(yaml.safe_dump(thread(name)))
    weave = tmp_path / "w.weave"
    weave.write_text(
        yaml.safe_dump(
            {
                "config_version": "1.0",
                "threads": [
                    {"ref": "a.thread", "dependencies": ["b"]},
                    {"ref": "b.thread", "dependencies": ["a"]},
                ],
            }
        )
    )
    (issue,) = load_resolved(weave, project_root=tmp_path).validate_plans()
    assert issue.diagnostic is not None
    assert (issue.diagnostic.code, issue.diagnostic.file) == ("D101", str(weave))
