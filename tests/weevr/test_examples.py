"""Load every example config through the real config loader.

Each ``.thread``, ``.weave``, and ``.loom`` under ``examples/`` must load with
``load_config`` against its project root, every ``.warp`` must validate, and
named warp references must resolve. A few examples also carry story checks so a
rewrite can't silently drop the feature the example exists to show.
"""

from __future__ import annotations

from pathlib import Path

import pytest

# Discovery, required params and project roots live in the core-only check
# script (on pytest's pythonpath) so both checks cover the same files.
from validate_examples_core import (
    CONFIG_FILES,
    EXAMPLES_DIR,
    WARP_FILES,
    WARP_NAMING_THREADS,
    check_warp_reference,
    load_example,
    rel,
    validate_warp,
)

from documents import write_document, write_project
from weevr.config import load_config, load_resolved
from weevr.errors import ConfigError, ModelValidationError, ReferenceResolutionError
from weevr.model import Loom, Thread, Weave


def _load(path: Path) -> Thread | Weave | Loom:
    model = load_example(path)
    assert isinstance(model, Thread | Weave | Loom)
    return model


def test_examples_discovered() -> None:
    """The globs find example configs and warps to check."""
    assert CONFIG_FILES, "no example configs found"
    assert WARP_FILES, "no example warps found"
    assert WARP_NAMING_THREADS, "no example thread names a warp"


@pytest.mark.parametrize("path", CONFIG_FILES, ids=rel)
def test_example_loads(path: Path) -> None:
    """Every example config loads through the real loader."""
    _load(path)


@pytest.mark.parametrize("path", WARP_FILES, ids=rel)
def test_warp_validates(path: Path) -> None:
    """Every example warp validates as a WarpConfig."""
    validate_warp(path)


@pytest.mark.parametrize("path", WARP_NAMING_THREADS, ids=rel)
def test_named_warp_reference_resolves(path: Path) -> None:
    """A thread that names its warp can find it from the project root."""
    check_warp_reference(path)


class TestExampleStories:
    """Examples keep the feature their README row describes."""

    def test_orders_pipeline_keeps_quality_gates(self) -> None:
        """The hooks example still has pre and post quality gates."""
        weave = _load(EXAMPLES_DIR / "orders_pipeline.weave")
        assert isinstance(weave, Weave)
        assert weave.pre_steps and weave.post_steps
        steps = [*weave.pre_steps, *weave.post_steps]
        assert any(step.type == "quality_gate" for step in steps)

    def test_enrichment_pipeline_keeps_both_lookup_kinds(self) -> None:
        """The lookups example keeps a materialized and an on-demand lookup, both used."""
        path = EXAMPLES_DIR / "enrichment_pipeline.weave"
        weave = _load(path)
        assert isinstance(weave, Weave)
        assert weave.lookups is not None
        assert {lookup.materialize for lookup in weave.lookups.values()} == {True, False}
        resolved = load_resolved(path, project_root=EXAMPLES_DIR).document.weave_threads()
        bound = {
            source.lookup
            for thread in resolved["enrichment_pipeline"].values()
            for source in thread.sources.values()
            if source.lookup is not None
        }
        assert bound == set(weave.lookups)

    def test_customer_pipeline_keeps_dependency_chain(self) -> None:
        """The DAG example keeps its explicit staging -> dim -> fact chain."""
        weave = _load(EXAMPLES_DIR / "customer_pipeline.weave")
        assert isinstance(weave, Weave)
        deps = {entry.name: entry.dependencies for entry in weave.threads}
        assert deps == {
            "staging_customers": None,
            "dim_customer": ["staging_customers"],
            "fact_customer_orders": ["dim_customer"],
        }

    def test_sales_lakehouse_keeps_weaves_params_and_defaults(self) -> None:
        """The loom example keeps two weaves, typed params, and defaults."""
        loom = _load(EXAMPLES_DIR / "sales_lakehouse.loom")
        assert isinstance(loom, Loom)
        assert len(loom.weaves) == 2
        assert loom.params and "lakehouse_path" in loom.params
        assert loom.defaults


class TestBrokenExamplesFail:
    """The load check catches the kinds of rot it exists for."""

    def _project(self, tmp_path: Path, files: dict[str, str]) -> Path:
        root = write_project(tmp_path / "p.weevr")
        for name, body in files.items():
            write_document(root / name, body)
        return root

    def test_name_not_matching_filename(self, tmp_path: Path) -> None:
        """A declared name that differs from the filename stem fails."""
        body = (EXAMPLES_DIR / "dim_store.thread").read_text()
        root = self._project(tmp_path, {"renamed.thread": body})
        with pytest.raises(ConfigError, match="does not match filename stem"):
            load_config(root / "renamed.thread", project_root=root)

    def test_invalid_step(self, tmp_path: Path) -> None:
        """A step in an outdated shape fails."""
        body = (
            'config_version: "2.0"\n'
            "sources:\n  s:\n    type: delta\n    alias: raw.s\n"
            "steps:\n  - operation: select\n    columns: [a]\n"
            "target:\n  alias: cur.s\n"
        )
        root = self._project(tmp_path, {"t.thread": body})
        with pytest.raises(ModelValidationError, match="C104 unknown-step-type"):
            load_config(root / "t.thread", project_root=root)

    def test_weave_entry_without_file_or_body(self, tmp_path: Path) -> None:
        """A weave entry that names a thread with no file and no inline body fails."""
        body = 'config_version: "2.0"\nthreads:\n  - name: ghost\n'
        root = self._project(tmp_path, {"w.weave": body})
        with pytest.raises(ReferenceResolutionError) as exc_info:
            load_config(root / "w.weave", project_root=root)
        assert [d.code for d in exc_info.value.diagnostics] == ["C313"]
