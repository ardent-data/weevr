"""The published v1 → v2 migration rules, proven against a golden project.

``reference_migrator`` implements ``docs/reference/migration-rules.md`` and
nothing else; it lives in ``tests/`` because the engine ships no migration
code. The v1 fixture project migrates byte-for-byte to the v2 golden, the
golden loads cleanly, and the v1 tree is rejected.
"""

from __future__ import annotations

import logging
import warnings
from pathlib import Path
from typing import Any

import pytest

from migration.reference_migrator import migrate_document, migrate_project, project_name
from weevr.config import load_config
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
