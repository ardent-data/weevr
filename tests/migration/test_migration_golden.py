"""The published v1 → v2 migration rules, proven against a golden project.

``reference_migrator`` implements ``docs/reference/migration-rules.md`` and
nothing else; it lives in ``tests/`` because the engine ships no migration
code. The v1 fixture project migrates byte-for-byte to the v2 golden, the
golden loads cleanly, and the v1 tree is rejected.
"""

from __future__ import annotations

import json
import logging
import re
import warnings
from pathlib import Path
from typing import Any

import pytest
import yaml

from migration.reference_migrator import (
    FABRIC_KEYS,
    NAMESPACES,
    Report,
    migrate_document,
    migrate_project,
    project_name,
)
from weevr.config import interpolation_errors, load_config, load_resolved
from weevr.config.discovery import discover_documents
from weevr.errors import ConfigError

FIXTURES = Path(__file__).parents[1] / "fixtures" / "migration"
V1 = FIXTURES / "v1" / "sales.weevr"
V2 = FIXTURES / "v2" / "sales.weevr"
PROJECT_ID = "7c1e5a52-3b8f-4d2a-9e61-0f4b8d2c9a17"


def _tree(root: Path) -> dict[str, bytes]:
    return {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def _migrate(source: Path, tmp_path: Path) -> Path:
    dest = tmp_path / source.name
    migrate_project(source, dest, project_id=PROJECT_ID)
    return dest


class TestGolden:
    def test_v1_project_migrates_to_the_v2_golden_byte_for_byte(self, tmp_path: Path):
        migrated = _tree(_migrate(V1, tmp_path))
        expected = _tree(V2)
        assert sorted(migrated) == sorted(expected)
        for path, content in expected.items():
            assert migrated[path] == content, path

    def test_migrating_the_v2_golden_changes_nothing(self, tmp_path: Path):
        assert _tree(_migrate(V2, tmp_path)) == _tree(V2)

    def test_non_document_files_are_copied_unchanged(self, tmp_path: Path):
        migrated = _tree(_migrate(V1, tmp_path))
        for path in ("README.md", "reference/codes.csv"):
            assert migrated[path] == (V1 / path).read_bytes()

    def test_v2_golden_loads_without_diagnostics(self, caplog: pytest.LogCaptureFixture):
        with warnings.catch_warnings(record=True) as caught, caplog.at_level(logging.WARNING):
            warnings.simplefilter("always")
            load_config(V2 / "sales.loom")
            for document in discover_documents(V2):
                if document.kind in ("loom", "weave", "thread", "params"):
                    load_config(V2 / document.path)
        assert caught == []
        assert [r for r in caplog.records if r.levelno >= logging.WARNING] == []

    def test_every_v2_document_declares_its_kind(self):
        kinds = {d.path: d.kind for d in discover_documents(V2)}
        assert kinds == {
            "dims.weave": "weave",
            "facts.weave": "weave",
            "params/prod.yaml": "params",
            "sales.loom": "loom",
            "threads/dims/dim_customer.thread": "thread",
            "threads/dims/dim_customer.warp": "warp",
            "threads/dims/dim_product.yaml": "thread",
            "threads/facts/fact_returns.thread": "thread",
            "threads/facts/fact_sales.thread": "thread",
            "weaves/returns.weave": "weave",
            "weevr.yaml": "project",
        }

    def test_v1_project_is_rejected_with_the_migration_code(self):
        with pytest.raises(ConfigError) as caught:
            load_config(V1 / "sales.loom")
        assert [d.code for d in caught.value.diagnostics] == ["C203"]


def _run(text: str, suffix: str, **kwargs: Any) -> str:
    """Migrate one document and check that migrating the result changes nothing."""
    out = migrate_document(text, suffix, **kwargs)
    assert migrate_document(out, suffix, **kwargs) == out
    return out


def _weave(text: str, existing: tuple[str, ...] = ("dims", "facts")) -> str:
    return _run(text, ".loom", weave_exists=lambda name: name in existing)


class TestKind:
    @pytest.mark.parametrize("suffix", [".thread", ".weave", ".loom", ".warp"])
    def test_typed_extension_names_the_kind(self, suffix: str):
        out = _run('config_version: "1.0"\ncolumns: []\n', suffix)
        assert out == f'kind: {suffix[1:]}\nconfig_version: "2.0"\ncolumns: []\n'

    @pytest.mark.parametrize(
        ("body", "kind"),
        [
            ("weaves: []\nthreads: []\n", "loom"),
            ("threads: []\nsources: {}\n", "weave"),
            ("sources: {}\n", "thread"),
            ("target: {}\n", "thread"),
            ("env: prod\n", "params"),
            ("type: weave\nsources: {}\n", "weave"),
        ],
    )
    def test_yaml_kind_follows_the_v1_detection_order(self, body: str, kind: str):
        out = _run(f'config_version: "1.0"\n{body}', ".yaml")
        assert out.startswith(f'kind: {kind}\nconfig_version: "2.0"\n')

    def test_yaml_with_only_a_version_is_not_migrated(self):
        assert _run('config_version: "1.0"\n', ".yml") == 'config_version: "1.0"\n'

    def test_kind_goes_after_leading_comments_and_the_document_marker(self):
        text = '# header\n\n%YAML 1.1\n---\n# about\nname: x\nconfig_version: "1.0"\n'
        out = _run(text + "sources: {}\n", ".thread")
        assert out == (
            '# header\n\n%YAML 1.1\n---\n# about\nkind: thread\nname: x\nconfig_version: "2.0"\n'
            "sources: {}\n"
        )


class TestVersionAndType:
    @pytest.mark.parametrize(
        ("before", "after"),
        [
            ('"1.0"', '"2.0"'),
            ("'1.3'", "'2.0'"),
            ("1.0", '"2.0"'),
            ('"1.10"  # note', '"2.0"  # note'),
        ],
    )
    def test_version_value_is_rewritten_in_place(self, before: str, after: str):
        out = _run(f"config_version: {before}\nsources: {{}}\n", ".thread")
        assert out == f"kind: thread\nconfig_version: {after}\nsources: {{}}\n"

    def test_non_v1_documents_are_unchanged(self):
        for text in ('config_version: "2.0"\nsources: {}\n', "sources: {}\n", "a: [\n"):
            assert _run(text, ".thread") == text

    def test_root_type_line_is_dropped_and_nested_type_kept(self):
        text = 'config_version: "1.0"\ntype: thread\nsources:\n  s:\n    type: delta\n'
        assert _run(text, ".thread") == (
            'kind: thread\nconfig_version: "2.0"\nsources:\n  s:\n    type: delta\n'
        )

    def test_root_type_that_is_not_a_kind_is_kept(self):
        text = 'config_version: "1.0"\ntype: full\nenv: prod\n'
        assert _run(text, ".yaml") == (
            'kind: params\nconfig_version: "2.0"\ntype: full\nenv: prod\n'
        )

    def test_line_endings_and_missing_final_newline_are_kept(self):
        text = 'config_version: "1.0"\r\nsources: {}\r\ntarget: {}'
        assert _run(text, ".thread") == (
            'kind: thread\r\nconfig_version: "2.0"\r\nsources: {}\r\ntarget: {}'
        )

    def test_byte_order_mark_stays_first(self):
        text = '\ufeffconfig_version: "1.0"\nsources: {}\n'
        assert _run(text, ".thread") == ('\ufeffkind: thread\nconfig_version: "2.0"\nsources: {}\n')


class TestNotMigrated:
    @pytest.mark.parametrize(
        "text",
        [
            '{config_version: "1.0", sources: {}}\n',
            '  config_version: "1.0"\n  sources: {}\n',
            '"config_version": "1.0"\nsources: {}\n',
            'config_version: "1.0"\n"type": thread\nsources: {}\n',
            'config_version: "1.0"\nsources:\n  s:\n    config_version: "1.0"\n',
        ],
        ids=["flow-root", "indented-root", "quoted-version", "quoted-type", "nested-version"],
    )
    def test_document_the_line_rules_cannot_migrate_is_unchanged(self, text: str):
        assert migrate_document(text, ".thread") == text

    def test_block_scalar_content_is_not_bumped(self):
        text = 'config_version: "1.0"\nsources: {}\nnote: |\n  config_version: 1.0\n'
        assert _run(text, ".thread") == (
            'kind: thread\nconfig_version: "2.0"\nsources: {}\nnote: |\n  config_version: 1.0\n'
        )

    def test_extension_matches_case_insensitively(self):
        assert _run('config_version: "1.0"\nsources: {}\n', ".THREAD").startswith("kind: thread\n")


class TestWeaveEntries:
    def test_bare_string_entry_becomes_a_ref(self):
        out = _weave('config_version: "1.0"\nweaves:\n- dims  # first\n- "facts"\n')
        assert out == (
            'kind: loom\nconfig_version: "2.0"\nweaves:\n- ref: dims.weave  # first\n'
            "- ref: facts.weave\n"
        )

    def test_name_only_entry_keeps_its_other_keys(self):
        text = (
            'config_version: "1.0"\nweaves:\n  - condition:\n      when: "true"\n    name: facts\n'
        )
        assert _weave(text) == (
            'kind: loom\nconfig_version: "2.0"\nweaves:\n  - condition:\n      when: "true"\n'
            "    ref: facts.weave\n"
        )

    def test_entry_without_a_weave_file_is_unchanged(self):
        text = 'config_version: "1.0"\nweaves:\n  - missing\n  - name: gone\n'
        assert _weave(text) == (
            'kind: loom\nconfig_version: "2.0"\nweaves:\n  - missing\n  - name: gone\n'
        )

    def test_inline_body_is_unchanged_but_its_version_is_bumped(self):
        text = (
            'config_version: "1.0"\nweaves:\n  - name: dims\n    config_version: "1.0"\n'
            "    threads: []\n"
        )
        assert _weave(text) == (
            'kind: loom\nconfig_version: "2.0"\nweaves:\n  - name: dims\n'
            '    config_version: "2.0"\n    threads: []\n'
        )

    def test_rewrite_keeps_crlf_line_endings(self):
        text = 'config_version: "1.0"\r\nweaves:\r\n  - dims\r\n  - name: facts\r\n'
        assert _weave(text) == (
            'kind: loom\r\nconfig_version: "2.0"\r\nweaves:\r\n  - ref: dims.weave\r\n'
            "  - ref: facts.weave\r\n"
        )

    def test_entry_with_keys_below_a_bare_dash(self):
        text = 'config_version: "1.0"\nweaves:\n  -\n    name: facts\n    condition: {when: "x"}\n'
        assert _weave(text) == (
            'kind: loom\nconfig_version: "2.0"\nweaves:\n  -\n    ref: facts.weave\n'
            '    condition: {when: "x"}\n'
        )

    def test_name_outside_the_plain_set_is_quoted(self):
        text = 'config_version: "1.0"\nweaves:\n  - "q 1"\n'
        assert _weave(text, existing=("q 1",)) == (
            'kind: loom\nconfig_version: "2.0"\nweaves:\n  - ref: "q 1.weave"\n'
        )


class TestProjectDocument:
    @pytest.mark.parametrize(
        ("folder", "name"),
        [
            ("sales.weevr", "sales"),
            ("sales", "sales"),
            ("2024 sales-eu.weevr", "project_2024_sales_eu"),
            ("_raw", "project__raw"),
            ("a" * 120, "a" * 100),
        ],
    )
    def test_name_comes_from_the_folder_stem(self, folder: str, name: str):
        assert project_name(folder) == name

    def test_existing_project_document_is_kept(self, tmp_path: Path):
        source = tmp_path / "src"
        source.mkdir()
        (source / "weevr.yaml").write_text("kind: project\n")
        migrate_project(source, tmp_path / "out", project_id=PROJECT_ID)
        assert (tmp_path / "out" / "weevr.yaml").read_text() == "kind: project\n"

    def test_hidden_platform_and_non_utf8_files_are_copied_unchanged(self, tmp_path: Path):
        source = tmp_path / "src"
        v1 = b'config_version: "1.0"\nsources: {}\n'
        files = {
            ".weevr/state.thread": v1,
            "threads/.platform": b"{}",
            "threads/latin.thread": b'config_version: "1.0"\n# caf\xe9\nsources: {}\n',
        }
        for path, content in files.items():
            (source / path).parent.mkdir(parents=True, exist_ok=True)
            (source / path).write_bytes(content)
        migrate_project(source, tmp_path / "out", project_id=PROJECT_ID)
        for path, content in files.items():
            assert (tmp_path / "out" / path).read_bytes() == content


VARS_V1 = FIXTURES / "v1" / "vars.weevr"
VARS_V2 = FIXTURES / "v2" / "vars.weevr"
VARS_RESOLVED = json.loads((FIXTURES / "vars_resolved.json").read_text())
# Runtime-only names in the vars fixture: rule 4 declares nothing, so the load warns.
VARS_FALLBACKS = {"storage.container", "run_date", "tier", "storage.region"}


def _vars_report(tmp_path: Path) -> Report:
    return migrate_project(VARS_V1, tmp_path / "vars.weevr", project_id=PROJECT_ID)


def _dig(data: Any, dotted: str) -> Any:
    for part in dotted.split("."):
        data = data[part]
    return data


class TestVariableReferencesGolden:
    def test_v1_project_migrates_to_the_v2_golden_byte_for_byte(self, tmp_path: Path):
        _vars_report(tmp_path)
        migrated = _tree(tmp_path / "vars.weevr")
        expected = _tree(VARS_V2)
        assert sorted(migrated) == sorted(expected)
        for path, content in expected.items():
            assert migrated[path] == content, path

    def test_migrating_the_v2_golden_changes_nothing(self, tmp_path: Path):
        report = migrate_project(VARS_V2, tmp_path / "vars.weevr", project_id=PROJECT_ID)
        assert _tree(tmp_path / "vars.weevr") == _tree(VARS_V2)
        assert report.rewrites == []

    def test_every_rule_applies_at_least_once(self, tmp_path: Path):
        report = _vars_report(tmp_path)
        assert {r.rule for r in report.rewrites} == {7, 8, 9, 10}
        assert any(r.dotted for r in report.rewrites)

    def test_fallbacks_are_listed_for_review(self, tmp_path: Path):
        report = _vars_report(tmp_path)
        assert {r.reference.strip("${}").partition(":")[0] for r in report.fallbacks} == (
            VARS_FALLBACKS
        )
        assert {r.file for r in report.fallbacks} == {"threads/orders.thread"}
        assert report.skipped == []

    def test_rewrites_name_the_line_of_the_v1_document(self, tmp_path: Path):
        for rewrite in _vars_report(tmp_path).rewrites:
            lines = (VARS_V1 / rewrite.file).read_text().splitlines()
            assert rewrite.reference in lines[rewrite.line - 1]

    def test_v1_vars_project_is_rejected_with_the_migration_code(self):
        with pytest.raises(ConfigError) as caught:
            load_config(VARS_V1 / "nightly.loom")
        assert [d.code for d in caught.value.diagnostics] == ["C203"]

    def test_v2_golden_warns_only_for_the_fallbacks(self):
        loaded = load_resolved(
            VARS_V2 / "nightly.loom",
            runtime_params=VARS_RESOLVED["scenarios"]["defaults"]["runtime_params"],
            fabric_context=VARS_RESOLVED["fabric_context"],
        )
        assert {d.code for d in loaded.diagnostics} == {"P120"}
        # Each P120 carries a ``params:`` block declaring the file's undeclared names.
        named = {name for d in loaded.diagnostics for name in yaml.safe_load(d.hint)["params"]}
        assert named == {"storage", "run_date"}

    @pytest.mark.parametrize("scenario", sorted(VARS_RESOLVED["scenarios"]))
    def test_v2_golden_resolves_to_the_values_v1_produced(self, scenario: str):
        case = VARS_RESOLVED["scenarios"][scenario]
        loaded = load_resolved(
            VARS_V2 / VARS_RESOLVED["load"],
            runtime_params=case["runtime_params"],
            fabric_context=VARS_RESOLVED["fabric_context"],
        )
        thread = loaded.document.weave_threads()[VARS_RESOLVED["weave"]][VARS_RESOLVED["thread"]]
        dumped = thread.model_dump(mode="json", exclude_none=True)
        root = str(VARS_V2.resolve())
        for path, expected in case["values"].items():
            actual = _dig(dumped, path)
            if isinstance(actual, str):
                actual = actual.replace(root, "<root>").replace(str(VARS_V2), "<root>")
            assert actual == expected, path


def _v1(body: str, *, root: str = "") -> str:
    return f'{root}config_version: "1.0"\n{body}'


def _thread(text: str, **kwargs: Any) -> tuple[str, Report]:
    report = Report()
    out = migrate_document(text, ".thread", report=report, file="t.thread", **kwargs)
    return out, report


class TestVariableReferenceRules:
    def test_first_match_wins_a_declared_param_beats_a_defaults_key(self):
        text = _v1(
            "params:\n  env:\n    name: env\n    type: string\n"
            "defaults:\n  env: prod\nsources:\n  a:\n    alias: ${env}\n"
        )
        out, report = _thread(text)
        assert "alias: ${param.env}" in out
        assert "env: prod" in out  # the defaults key is not converted
        assert [r.rule for r in report.rewrites] == [7]

    def test_a_defaults_key_beats_a_fabric_key(self):
        text = _v1("defaults:\n  workspace_id: abc\ntarget:\n  alias: ${workspace_id}\n")
        out, report = _thread(text)
        assert "alias: ${param.workspace_id}" in out
        assert [r.rule for r in report.rewrites] == [8]

    @pytest.mark.parametrize("key", ["workspace_id", "lakehouse_id", "workspace_name"])
    def test_fabric_keys_map_to_the_fabric_namespace(self, key: str):
        out, report = _thread(_v1(f"target:\n  alias: ${{{key}}}\n"))
        assert f"alias: ${{fabric.{key}}}" in out
        assert [r.rule for r in report.rewrites] == [9]

    def test_a_runtime_only_name_is_a_param_and_a_reported_fallback(self):
        out, report = _thread(_v1("target:\n  alias: ${region}\n"))
        assert "alias: ${param.region}" in out
        assert [(r.rule, r.dotted) for r in report.rewrites] == [(10, False)]
        assert report.fallbacks == report.rewrites

    def test_the_fallback_text_is_kept_through_every_rewrite(self):
        text = _v1(
            "defaults:\n  env: prod\ntarget:\n"
            "  alias: ${env:-dev}_${workspace_id:-x}_${other:-a b}\n"
        )
        out, _ = _thread(text)
        assert "alias: ${param.env:-dev}_${fabric.workspace_id:-x}_${param.other:-a b}" in out

    def test_a_dotted_name_is_classified_by_its_first_segment(self):
        text = _v1(
            "defaults:\n  env: prod\ntarget:\n"
            "  alias: ${env.lakehouse}_${cfg.name:-n}_${workspace_id.x}\n"
        )
        out, report = _thread(text)
        assert "${param.env.lakehouse}_${param.cfg.name:-n}_${param.workspace_id.x}" in out
        assert [(r.rule, r.dotted) for r in report.rewrites] == [(8, True), (10, True), (10, True)]

    @pytest.mark.parametrize(
        "reference",
        [
            "${param.x}",
            "${fabric.workspace_id}",
            "${var.x}",
            "${run.id}",
            "${thread.name}",
            "${weave.name}",
            "${loom.name}",
        ],
    )
    def test_namespaced_references_are_untouched(self, reference: str):
        text = _v1(f"target:\n  alias: {reference}\n")
        out, report = _thread(text)
        assert out == f'kind: thread\nconfig_version: "2.0"\ntarget:\n  alias: {reference}\n'
        assert report.rewrites == []

    def test_references_in_comments_are_untouched_but_block_scalars_are_rewritten(self):
        text = _v1("target:\n  alias: ${a}  # also ${b}\nsql: |\n  select ${c}  -- ${d}\n")
        out, report = _thread(text)
        assert "alias: ${param.a}  # also ${b}" in out
        assert "select ${param.c}  -- ${param.d}" in out
        assert [r.reference for r in report.rewrites] == ["${a}", "${c}", "${d}"]

    @pytest.mark.parametrize(
        ("value", "type_name", "default"),
        [
            ("true", "bool", "true"),
            ("500", "int", "500"),
            ("1.5", "float", "1.5"),
            ("prod", "string", "prod"),
            ('"2024-01-01"', "string", '"2024-01-01"'),
            ("2024-01-01", "string", '"2024-01-01"'),
            ("[a, b]", "list[string]", "[a, b]"),
        ],
    )
    def test_rule_two_infers_the_type_from_the_value(
        self, value: str, type_name: str, default: str
    ):
        out, _ = _thread(_v1(f"defaults:\n  x: {value}\ntarget:\n  alias: ${{x}}\n"))
        assert (
            f"params:\n  x:\n    name: x\n    type: {type_name}\n    default: {default}\n"
        ) in out
        assert "defaults:" not in out

    def test_rule_two_appends_to_an_existing_params_block_and_keeps_other_defaults(self):
        text = _v1(
            "params:\n  a:\n    name: a\n    type: int\n"
            "defaults:\n  x: 1\n  y: 2\ntarget:\n  alias: ${x}\n"
        )
        out, _ = _thread(text)
        assert out == (
            'kind: thread\nconfig_version: "2.0"\n'
            "params:\n  a:\n    name: a\n    type: int\n"
            "  x:\n    name: x\n    type: int\n    default: 1\n"
            "defaults:\n  y: 2\ntarget:\n  alias: ${param.x}\n"
        )

    def test_a_defaults_key_nothing_references_stays_in_defaults(self):
        text = _v1("defaults:\n  x: 1\ntarget:\n  alias: a\n")
        assert _thread(text)[0].count("defaults:") == 1

    @pytest.mark.parametrize(
        "block",
        [
            "defaults: {x: 1}\n",  # flow mapping
            "defaults:\n  x:\n    - a\n",  # block list
            "defaults:\n  x: null\n",
            "defaults:\n  x: 1\nparams: {}\n",  # params the edit cannot extend
        ],
    )
    def test_a_defaults_key_rule_eight_cannot_convert_falls_back_to_rule_ten(self, block: str):
        out, report = _thread(_v1(block + "target:\n  alias: ${x}\n"))
        assert "alias: ${param.x}" in out
        assert [(r.rule, r.dotted) for r in report.rewrites] == [(10, False)]

    def test_a_defaults_key_of_an_ancestor_is_converted_in_the_ancestor(self, tmp_path: Path):
        source = tmp_path / "p.weevr"
        (source / "threads").mkdir(parents=True)
        (source / "a.loom").write_text(_v1("defaults:\n  env: prod\nweaves:\n  - w\n"))
        (source / "w.weave").write_text(_v1("threads:\n  - ref: threads/t.thread\n"))
        (source / "threads" / "t.thread").write_text(_v1("sources:\n  s:\n    alias: ${env}\n"))
        dest = tmp_path / "out"
        report = migrate_project(source, dest, project_id=PROJECT_ID)
        assert "params:\n  env:" in (dest / "a.loom").read_text()
        assert "${param.env}" in (dest / "threads" / "t.thread").read_text()
        assert [(r.file, r.rule) for r in report.rewrites] == [("threads/t.thread", 8)]


BARE = re.compile(r"\$\{(?!(?:" + "|".join(NAMESPACES) + r")\.)[^}]*\}")
CORPUS_SUFFIXES = {".thread", ".weave", ".loom", ".warp", ".yaml", ".yml"}


def _corpus() -> list[Path]:
    repo = FIXTURES.parents[2]
    roots = [repo / "tests", repo / "examples"]
    return sorted(
        p
        for root in roots
        for p in root.rglob("*")
        if p.is_file() and p.suffix.lower() in CORPUS_SUFFIXES
    )


class TestCorpus:
    def test_every_bare_reference_in_the_fixture_trees_maps_to_a_rule(self):
        v1_root = FIXTURES / "v1"
        found = 0
        for path in _corpus():
            text = path.read_text(encoding="utf-8", errors="ignore")
            bare = BARE.findall(text)
            if not bare:
                continue
            found += len(bare)
            assert path.is_relative_to(v1_root), f"bare reference outside the v1 tree: {path}"
            report = Report()
            out = migrate_document(text, path.suffix, report=report, file=path.name)
            assert out != text, f"{path} was not migrated"
            assert len(report.rewrites) == len(bare), path
            assert {r.rule for r in report.rewrites} <= {7, 8, 9, 10}
            assert not BARE.findall(out), path
        assert found > 0

    def test_the_project_report_covers_every_reference_in_the_v1_trees(self, tmp_path: Path):
        for project in sorted((FIXTURES / "v1").iterdir()):
            expected = sum(
                len(BARE.findall(p.read_text(encoding="utf-8", errors="ignore")))
                for p in project.rglob("*")
                if p.is_file()
            )
            report = migrate_project(project, tmp_path / project.name, project_id=PROJECT_ID)
            assert len(report.rewrites) == expected, project.name


class TestDrift:
    def test_the_migrator_reads_the_engines_namespaces_and_fabric_keys(self):
        assert tuple(NAMESPACES) == interpolation_errors.NAMESPACES
        assert frozenset(FABRIC_KEYS) == interpolation_errors.FABRIC_KEYS


class TestLimits:
    def test_a_reference_in_a_mapping_key_leaves_the_document_unmigrated_and_reported(self):
        text = _v1("target:\n  alias: gold.${x}\n  audit_columns:\n    ${y}: a\n")
        report = Report()
        out = migrate_document(text, ".thread", report=report, file="t.thread")
        assert out == text
        assert report.skipped == ["t.thread"]
        assert report.rewrites == []

    def test_a_nested_fallback_rewrites_the_outer_reference_only(self):
        out, report = _thread(_v1("target:\n  alias: ${x:-${y}}\n"))
        assert "alias: ${param.x:-${y}}" in out
        assert [r.reference for r in report.rewrites] == ["${x:-${y}"]
        assert report.skipped == []

    @pytest.mark.parametrize(
        ("value", "type_name"),
        [
            ("yes", "bool"),
            ("0x1F", "int"),
            ("1_000", "int"),
            ("1e3", "string"),
            ("1.0e+3", "float"),
        ],
    )
    def test_rule_eight_types_a_value_as_a_yaml_1_1_parser_reads_it_and_keeps_its_text(
        self, value: str, type_name: str
    ):
        out, _ = _thread(_v1(f"defaults:\n  x: {value}\ntarget:\n  alias: ${{x}}\n"))
        assert f"    type: {type_name}\n    default: {value}\n" in out

    def test_a_defaults_key_of_an_inline_scope_is_not_converted(self):
        text = _v1(
            "weaves:\n  - name: w\n    defaults:\n      x: 1\n    threads:\n"
            "      - name: t\n        target:\n          alias: ${x}\n"
        )
        report = Report()
        out = migrate_document(text, ".loom", report=report, file="a.loom")
        assert "      x: 1\n" in out
        assert "params:" not in out
        assert [(r.rule, r.dotted) for r in report.rewrites] == [(10, False)]


class TestChains:
    def _project(self, tmp_path: Path, files: dict[str, str]) -> tuple[Path, Report]:
        source = tmp_path / "p.weevr"
        for name, body in files.items():
            (source / name).parent.mkdir(parents=True, exist_ok=True)
            (source / name).write_text(_v1(body))
        return source, migrate_project(source, tmp_path / "out", project_id=PROJECT_ID)

    def test_the_nearest_parent_by_path_wins_when_parents_disagree(self, tmp_path: Path):
        _, report = self._project(
            tmp_path,
            {
                "b.loom": "defaults:\n  env: b\nweaves:\n  - w\n",
                "a.loom": "defaults:\n  env: a\nweaves:\n  - w\n",
                "w.weave": "threads:\n  - ref: t.thread\n",
                "t.thread": "target:\n  alias: ${env}\n",
            },
        )
        out = tmp_path / "out"
        assert "default: a" in (out / "a.loom").read_text()
        assert "defaults:\n  env: b" in (out / "b.loom").read_text()
        assert [(r.file, r.rule) for r in report.rewrites] == [("t.thread", 8)]

    def test_a_reference_cycle_ends_the_chain(self, tmp_path: Path):
        _, report = self._project(
            tmp_path,
            {
                "a.weave": "defaults:\n  x: 1\nthreads:\n  - ref: b.weave\n",
                "b.weave": "threads:\n  - ref: a.weave\n  - ref: t.thread\n",
                "t.thread": "target:\n  alias: ${x}\n",
            },
        )
        assert [r.rule for r in report.rewrites] == [8]


class TestIndentedDefaults:
    def test_a_four_space_defaults_block_keeps_its_indent_and_surviving_keys(self):
        text = (VARS_V2 / "threads/indented.thread").read_text()
        assert "params:\n    batch:\n        name: batch\n" in text
        assert "\n\ndefaults:\n    note: kept\n" in text

    def test_the_indented_thread_loads(self):
        loaded = load_resolved(VARS_V2 / "threads/indented.thread")
        assert loaded.model.target.alias == "gold.items_10"


class TestParamsBlockAtEndOfFile:
    def test_extending_a_params_block_at_the_end_of_a_file_keeps_a_missing_final_newline(self):
        text = (
            'config_version: "1.0"\ndefaults:\n  x: 1\ntarget:\n  alias: ${x}\n'
            "params:\n  a:\n    name: a\n    type: int"
        )
        out, _ = _thread(text)
        assert out.endswith("    type: int\n  x:\n    name: x\n    type: int\n    default: 1")
