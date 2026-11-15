"""A malformed config fails to load with a located ``ConfigError``, whatever the entry point.

Each mutation breaks one file of the ``forms`` fixture project (see
``tests/weevr/test_load_parity.py``) and loads a config that reaches it through
``load_config``, ``load_resolved`` and ``Context.load``. Every load must raise
a :class:`~weevr.errors.ConfigError` subclass that names a file, never a bare
``KeyError``, ``TypeError`` or ``AttributeError``.
"""

from __future__ import annotations

import shutil
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
import yaml
from pyspark.sql import SparkSession

from weevr.config import load_config, load_resolved
from weevr.context import Context
from weevr.errors import ConfigError

FORMS = Path(__file__).parents[1] / "fixtures" / "load_parity" / "forms.weevr"

Edit = Callable[[dict[str, Any]], None]


@dataclass(frozen=True)
class Mutation:
    """One way to break one file of the fixture project."""

    id: str
    file: str
    edit: Edit | str
    load: str = "forms.loom"

    def apply(self, root: Path) -> None:
        path = root / self.file
        if isinstance(self.edit, str):
            path.write_text(self.edit)
            return
        data = yaml.safe_load(path.read_text())
        self.edit(data)
        path.write_text(yaml.safe_dump(data, sort_keys=False))


def _set(*keys: str, value: Any) -> Edit:
    def edit(data: dict[str, Any]) -> None:
        node: Any = data
        for key in keys[:-1]:
            node = node[int(key)] if isinstance(node, list) else node[key]
        if isinstance(node, list):
            node[int(keys[-1])] = value
        else:
            node[keys[-1]] = value

    return edit


def _drop(*keys: str) -> Edit:
    def edit(data: dict[str, Any]) -> None:
        node: Any = data
        for key in keys[:-1]:
            node = node[int(key)] if isinstance(node, list) else node[key]
        del node[keys[-1]]

    return edit


def _append(*keys: str, value: Any) -> Edit:
    def edit(data: dict[str, Any]) -> None:
        node: Any = data
        for key in keys:
            node = node[int(key)] if isinstance(node, list) else node[key]
        node.append(value)

    return edit


CORE = "weaves/core.weave"
BASE = "threads/base.thread"

MUTATIONS: list[Any] = [
    # Removed required keys
    Mutation("loom-no-version", "forms.loom", _drop("config_version")),
    Mutation("weave-no-version", CORE, _drop("config_version")),
    Mutation("thread-no-version", BASE, _drop("config_version")),
    Mutation("thread-no-target", BASE, _drop("target")),
    Mutation("thread-no-sources", BASE, _drop("sources")),
    Mutation("inline-thread-no-target", CORE, _drop("threads", "3", "target")),
    Mutation("inline-weave-no-threads", "forms.loom", _drop("weaves", "2", "threads")),
    Mutation("thread-standalone-no-target", BASE, _drop("target"), load=BASE),
    # Wrong types at each level
    Mutation("loom-weaves-string", "forms.loom", _set("weaves", value="core")),
    Mutation("loom-weaves-entry-int", "forms.loom", _set("weaves", "0", value=5)),
    Mutation("loom-lookups-list", "forms.loom", _set("lookups", value=["regions"])),
    Mutation("loom-defaults-string", "forms.loom", _set("defaults", value="gold")),
    Mutation("loom-params-list", "forms.loom", _set("params", value=["env"])),
    Mutation("loom-column-sets-string", "forms.loom", _set("column_sets", value="x")),
    Mutation("loom-variables-int", "forms.loom", _set("variables", value=3)),
    Mutation("loom-condition-string", "forms.loom", _set("weaves", "0", "condition", value="x")),
    Mutation("weave-threads-mapping", CORE, _set("threads", value={"base": 1})),
    Mutation("weave-threads-entry-int", CORE, _set("threads", "0", value=5)),
    Mutation("weave-defaults-list", CORE, _set("defaults", value=["write"])),
    Mutation("weave-lookups-string", CORE, _set("lookups", value="shared")),
    Mutation("weave-deps-string", CORE, _set("threads", "1", "dependencies", value="base")),
    Mutation("thread-sources-list", BASE, _set("sources", value=["orders"])),
    Mutation("thread-steps-string", BASE, _set("steps", value="filter")),
    Mutation("thread-step-int", BASE, _set("steps", "1", value=5)),
    Mutation("thread-target-int", BASE, _set("target", value=5)),
    Mutation("thread-lookups-list", BASE, _set("lookups", value=["shared"])),
    Mutation("thread-variables-string", BASE, _set("variables", value="x")),
    Mutation("thread-column-sets-list", BASE, _set("column_sets", value=["m"])),
    Mutation("document-is-list", BASE, "- config_version: '1.0'\n"),
    Mutation("document-is-scalar", BASE, "just text\n"),
    Mutation("document-is-empty", BASE, ""),
    # Unknown child references
    Mutation("missing-thread-ref", CORE, _set("threads", "0", "ref", value="threads/nope.thread")),
    Mutation("missing-weave-ref", "forms.loom", _set("weaves", "0", "ref", value="nope.weave")),
    Mutation("missing-convention-weave", "forms.loom", _set("weaves", "1", value={"name": "nope"})),
    Mutation("ref-wrong-extension", CORE, _set("threads", "0", "ref", value="threads/base.txt")),
    Mutation("ref-not-string", CORE, _set("threads", "0", "ref", value=5)),
    Mutation("missing-lookup", BASE, _set("sources", "region", "lookup", value="nope")),
    Mutation("missing-column-set", BASE, _set("steps", "1", "rename", "column_set", value="nope")),
    # Cyclic references
    Mutation("loom-refs-itself", "forms.loom", _set("weaves", "0", "ref", value="forms.loom")),
    # (A thread dependency cycle is not a load error: plan and validate report it.)
    # Bad YAML
    Mutation("bad-yaml-child", BASE, "config_version: '1.0'\nsources: [unclosed\n"),
    Mutation("bad-yaml-top", "forms.loom", "config_version: '1.0'\nweaves:\n\t- ref: x\n"),
    Mutation("duplicate-yaml-key", BASE, "config_version: '1.0'\ntarget: {}\ntarget: {}\n"),
    # Bad as / params
    Mutation("template-missing-param", CORE, _drop("threads", "1", "params")),
    Mutation("template-ref-twice-no-as", CORE, _drop("threads", "2", "as")),
    Mutation("as-not-string", CORE, _set("threads", "1", "as", value=5)),
    Mutation("params-not-mapping", CORE, _set("threads", "1", "params", value="a")),
    Mutation("params-unresolved", CORE, _set("threads", "1", "params", "suffix", value="${x}")),
    Mutation("param-wrong-type", "forms.loom", _set("params", "env", "type", value="blob")),
    # Bad foreach
    Mutation("foreach-no-values", BASE, _drop("steps", "0", "foreach", "values")),
    pytest.param(
        Mutation(
            "foreach-values-string", BASE, _set("steps", "0", "foreach", "values", value="ab")
        ),
        marks=pytest.mark.xfail(
            strict=True,
            reason="loads without error: a string is iterated as its characters (load audit)",
        ),
        id="foreach-values-string",
    ),
    Mutation("foreach-steps-missing", BASE, _drop("steps", "0", "foreach", "steps")),
    Mutation("foreach-not-mapping", BASE, _set("steps", "0", "foreach", value=["a"])),
    # Duplicate names
    Mutation("duplicate-as", CORE, _set("threads", "2", "as", value="tpl_a")),
    Mutation("duplicate-inline-thread", CORE, _append("threads", value={"name": "inline_core"})),
    Mutation(
        "duplicate-weave", "forms.loom", _append("weaves", value={"ref": "weaves/core.weave"})
    ),
    Mutation("thread-name-mismatch", BASE, _set("name", value="other"), load=BASE),
]


def _load_config(root: Path, config: str) -> None:
    load_config(root / config, project_root=root)


def _load_resolved(root: Path, config: str) -> None:
    load_resolved(root / config, project_root=root)


def _context_load(root: Path, config: str) -> None:
    Context(spark=MagicMock(spec=SparkSession), project=str(root)).load(config)


ENTRY_POINTS = {
    "load_config": _load_config,
    "load_resolved": _load_resolved,
    "Context.load": _context_load,
}


def test_fixture_loads_unmutated(tmp_path: Path) -> None:
    """The unmutated project loads, so every failure below is the mutation's."""
    root = tmp_path / "forms.weevr"
    shutil.copytree(FORMS, root)
    for load in ENTRY_POINTS.values():
        load(root, "forms.loom")
        load(root, BASE)


@pytest.mark.parametrize("entry", ENTRY_POINTS, ids=str)
@pytest.mark.parametrize(
    "mutation", MUTATIONS, ids=lambda m: m.id if isinstance(m, Mutation) else None
)
def test_malformed_config_raises_located_config_error(
    mutation: Mutation, entry: str, tmp_path: Path
) -> None:
    root = tmp_path / "forms.weevr"
    shutil.copytree(FORMS, root)
    mutation.apply(root)
    with pytest.raises(ConfigError) as caught:
        ENTRY_POINTS[entry](root, mutation.load)
    assert caught.value.file_path, f"{type(caught.value).__name__} names no file: {caught.value}"
