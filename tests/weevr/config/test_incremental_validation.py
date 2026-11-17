"""Tests for incremental config cross-cutting validation."""

from pathlib import Path

import pytest

from weevr.config import load_config, load_resolved
from weevr.config.validation import render_diagnostics, validate_incremental_config
from weevr.errors import ConfigError


class TestValidateIncrementalConfig:
    """Test validate_incremental_config diagnostics."""

    def test_incremental_parameter_with_param_ref_valid(self):
        """incremental_parameter with ${param.} in steps is valid (no diagnostics)."""
        config = {
            "load": {"mode": "incremental_parameter"},
            "sources": {"src": {"type": "delta", "alias": "t"}},
            "steps": [{"filter": "date > '${param.start_date}'"}],
            "target": {"alias": "out"},
        }
        diags = validate_incremental_config(config)
        assert not any("WARN" in d for d in diags)

    def test_incremental_parameter_no_param_ref_warns(self):
        """incremental_parameter without ${param.} references produces warning."""
        config = {
            "load": {"mode": "incremental_parameter"},
            "sources": {"src": {"type": "delta", "alias": "t"}},
            "steps": [{"filter": "date > '2024-01-01'"}],
            "target": {"alias": "out"},
        }
        diags = validate_incremental_config(config)
        assert any("WARN" in d and "incremental_parameter" in d for d in diags)

    def test_cdc_with_merge_valid(self):
        """cdc mode with write.mode=merge produces no error."""
        config = {
            "load": {"mode": "cdc"},
            "write": {"mode": "merge", "match_keys": ["id"]},
            "sources": {"src": {"type": "delta", "alias": "t"}},
            "target": {"alias": "out"},
        }
        diags = validate_incremental_config(config)
        assert not any("ERROR" in d for d in diags)

    def test_cdc_with_overwrite_errors(self):
        """cdc mode with write.mode=overwrite produces error."""
        config = {
            "load": {"mode": "cdc"},
            "write": {"mode": "overwrite"},
            "sources": {"src": {"type": "delta", "alias": "t"}},
            "target": {"alias": "out"},
        }
        diags = validate_incremental_config(config)
        assert any("ERROR" in d and "cdc" in d and "merge" in d for d in diags)

    def test_incremental_watermark_with_explicit_overwrite_errors(self):
        """incremental_watermark with write.mode=overwrite produces error."""
        config = {
            "load": {"mode": "incremental_watermark", "watermark_column": "updated_at"},
            "write": {"mode": "overwrite"},
            "sources": {"src": {"type": "delta", "alias": "t"}},
            "target": {"alias": "out"},
        }
        diags = validate_incremental_config(config)
        errors = [d for d in diags if "ERROR" in d and "incremental_watermark" in d]
        assert errors
        # Explicitly configured overwrite must not be blamed on the default
        assert all("(the default)" not in d for d in errors)

    def test_incremental_watermark_with_defaulted_overwrite_errors(self):
        """incremental_watermark with no write block (defaulted overwrite) produces error."""
        config = {
            "load": {"mode": "incremental_watermark", "watermark_column": "updated_at"},
            "sources": {"src": {"type": "delta", "alias": "t"}},
            "target": {"alias": "out"},
        }
        diags = validate_incremental_config(config)
        assert any(
            "ERROR" in d and "incremental_watermark" in d and "(the default)" in d for d in diags
        )

    def test_incremental_watermark_with_append_no_error(self):
        """incremental_watermark with write.mode=append produces no error."""
        config = {
            "load": {"mode": "incremental_watermark", "watermark_column": "updated_at"},
            "write": {"mode": "append"},
            "sources": {"src": {"type": "delta", "alias": "t"}},
            "target": {"alias": "out"},
        }
        diags = validate_incremental_config(config)
        assert not any("ERROR" in d for d in diags)

    def test_incremental_watermark_with_merge_no_error(self):
        """incremental_watermark with write.mode=merge produces no error."""
        config = {
            "load": {"mode": "incremental_watermark", "watermark_column": "updated_at"},
            "write": {"mode": "merge", "match_keys": ["id"]},
            "sources": {"src": {"type": "delta", "alias": "t"}},
            "target": {"alias": "out"},
        }
        diags = validate_incremental_config(config)
        assert not any("ERROR" in d for d in diags)

    def test_cdc_update_value_without_watermark_errors(self):
        """Generic CDC with update_value but no watermark_column produces error."""
        config = {
            "load": {
                "mode": "cdc",
                "cdc": {"operation_column": "op", "insert_value": "I", "update_value": "U"},
            },
            "write": {"mode": "merge", "match_keys": ["id"]},
            "sources": {"src": {"type": "delta", "alias": "t"}},
            "target": {"alias": "out"},
        }
        diags = validate_incremental_config(config)
        assert any("ERROR" in d and "watermark_column" in d for d in diags)

    def test_cdc_delete_value_without_watermark_errors(self):
        """Generic CDC with delete_value but no watermark_column produces error."""
        config = {
            "load": {
                "mode": "cdc",
                "cdc": {"operation_column": "op", "insert_value": "I", "delete_value": "D"},
            },
            "write": {"mode": "merge", "match_keys": ["id"]},
            "sources": {"src": {"type": "delta", "alias": "t"}},
            "target": {"alias": "out"},
        }
        diags = validate_incremental_config(config)
        assert any("ERROR" in d and "watermark_column" in d for d in diags)

    def test_cdc_insert_only_without_watermark_no_error(self):
        """Insert-only generic CDC needs no ordering column."""
        config = {
            "load": {
                "mode": "cdc",
                "cdc": {"operation_column": "op", "insert_value": "I"},
            },
            "write": {"mode": "merge", "match_keys": ["id"]},
            "sources": {"src": {"type": "delta", "alias": "t"}},
            "target": {"alias": "out"},
        }
        diags = validate_incremental_config(config)
        assert not any("ERROR" in d and "watermark_column" in d for d in diags)

    def test_cdc_update_value_with_watermark_no_error(self):
        """Generic CDC with update_value and a watermark_column passes."""
        config = {
            "load": {
                "mode": "cdc",
                "watermark_column": "updated_at",
                "watermark_type": "timestamp",
                "cdc": {"operation_column": "op", "insert_value": "I", "update_value": "U"},
            },
            "write": {"mode": "merge", "match_keys": ["id"]},
            "sources": {"src": {"type": "delta", "alias": "t"}},
            "target": {"alias": "out"},
        }
        diags = validate_incremental_config(config)
        assert not any("ERROR" in d and "watermark_column" in d for d in diags)

    def test_cdc_cdf_preset_without_watermark_no_error(self):
        """The delta_cdf preset orders by commit version; no watermark needed."""
        config = {
            "load": {"mode": "cdc", "cdc": {"preset": "delta_cdf"}},
            "write": {"mode": "merge", "match_keys": ["id"]},
            "sources": {"src": {"type": "delta", "alias": "t"}},
            "target": {"alias": "out"},
        }
        diags = validate_incremental_config(config)
        assert not any("ERROR" in d and "watermark_column" in d for d in diags)

    def test_watermark_inclusive_with_append_warns(self):
        """watermark_inclusive=True with append produces warning."""
        config = {
            "load": {"mode": "incremental_watermark", "watermark_inclusive": True},
            "write": {"mode": "append"},
            "sources": {"src": {"type": "delta", "alias": "t"}},
            "target": {"alias": "out"},
        }
        diags = validate_incremental_config(config)
        assert any("WARN" in d and "watermark_inclusive" in d for d in diags)

    def test_watermark_inclusive_with_merge_no_warning(self):
        """watermark_inclusive=True with merge produces no warning."""
        config = {
            "load": {"mode": "incremental_watermark", "watermark_inclusive": True},
            "write": {"mode": "merge", "match_keys": ["id"]},
            "sources": {"src": {"type": "delta", "alias": "t"}},
            "target": {"alias": "out"},
        }
        diags = validate_incremental_config(config)
        assert not any("WARN" in d and "watermark_inclusive" in d for d in diags)

    def test_full_mode_no_diagnostics(self):
        """Full mode produces no diagnostics."""
        config = {
            "load": {"mode": "full"},
            "sources": {"src": {"type": "delta", "alias": "t"}},
            "target": {"alias": "out"},
        }
        diags = validate_incremental_config(config)
        assert len(diags) == 0

    def test_no_load_block_no_diagnostics(self):
        """No load block produces no diagnostics."""
        config = {
            "sources": {"src": {"type": "delta", "alias": "t"}},
            "target": {"alias": "out"},
        }
        diags = validate_incremental_config(config)
        assert len(diags) == 0

    def test_incremental_parameter_with_param_in_sources(self):
        """incremental_parameter with ${param.} in sources is valid."""
        config = {
            "load": {"mode": "incremental_parameter"},
            "sources": {"src": {"type": "delta", "alias": "${param.table_path}"}},
            "steps": [],
            "target": {"alias": "out"},
        }
        diags = validate_incremental_config(config)
        assert not any("WARN" in d and "incremental_parameter" in d for d in diags)


# ---------------------------------------------------------------------------
# The checks run in the shared load, for every thread it pulls in
# ---------------------------------------------------------------------------


_CDC_OVERWRITE = """\
config_version: "1.0"
sources:
  src: {type: delta, alias: raw.orders}
target: {alias: silver.orders}
load:
  mode: cdc
  cdc: {preset: delta_cdf}
"""

_PARAM_THREAD = """\
config_version: "1.0"
params:
  start: {name: start, type: string, default: "2024-01-01"}
sources:
  src: {type: delta, alias: raw.orders}
steps:
  - filter: {expr: "order_date >= '${param.start}'"}
target: {alias: silver.orders}
write: {mode: append}
load: {mode: incremental_parameter}
"""

_PARAM_FREE_THREAD = """\
config_version: "1.0"
sources:
  src: {type: delta, alias: raw.orders}
steps:
  - filter: {expr: "order_date >= '2024-01-01'"}
target: {alias: silver.orders}
write: {mode: append}
load: {mode: incremental_parameter}
"""


def _project(tmp_path: Path, files: dict[str, str]) -> Path:
    root = tmp_path / "inc.weevr"
    for name, text in files.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
    return root


def _context_load(root: Path, config: str) -> object:
    from unittest.mock import MagicMock

    from pyspark.sql import SparkSession

    from weevr.context import Context

    return Context(spark=MagicMock(spec=SparkSession), project=str(root)).load(config)


_ENTRY_POINTS = {
    "load_config": lambda root, config: load_config(root / config, project_root=root),
    "load_resolved": lambda root, config: load_resolved(root / config, project_root=root),
    "Context.load": _context_load,
}


class TestIncrementalChecksAtLoad:
    """Invalid incremental settings fail at load with a located ConfigError."""

    @pytest.mark.parametrize("entry", _ENTRY_POINTS)
    def test_standalone_thread(self, entry: str, tmp_path: Path) -> None:
        root = _project(tmp_path, {"t.thread": _CDC_OVERWRITE})
        with pytest.raises(
            ConfigError, match=r"thread 't'.*cdc mode requires write.mode='merge'"
        ) as e:
            _ENTRY_POINTS[entry](root, "t.thread")
        assert e.value.file_path == str(root / "t.thread")

    @pytest.mark.parametrize("entry", _ENTRY_POINTS)
    def test_child_thread_names_its_own_file(self, entry: str, tmp_path: Path) -> None:
        root = _project(
            tmp_path,
            {
                "threads/t.thread": _CDC_OVERWRITE,
                "w.weave": 'config_version: "1.0"\nthreads:\n  - ref: threads/t.thread\n',
                "l.loom": 'config_version: "1.0"\nweaves:\n  - ref: w.weave\n',
            },
        )
        for config in ("w.weave", "l.loom"):
            with pytest.raises(ConfigError, match=r"thread 't'.*cdc mode") as e:
                _ENTRY_POINTS[entry](root, config)
            assert e.value.file_path == str(root / "threads" / "t.thread")

    def test_inline_thread_names_the_enclosing_file(self, tmp_path: Path) -> None:
        weave = (
            'config_version: "1.0"\n'
            "threads:\n"
            "  - name: t\n"
            '    config_version: "1.0"\n'
            "    sources: {src: {type: delta, alias: raw.orders}}\n"
            "    target: {alias: silver.orders}\n"
            "    load: {mode: cdc, cdc: {preset: delta_cdf}}\n"
        )
        root = _project(tmp_path, {"w.weave": weave})
        with pytest.raises(ConfigError, match=r"thread 't'.*cdc mode") as e:
            load_resolved(root / "w.weave", project_root=root)
        assert e.value.file_path == str(root / "w.weave")

    def test_weave_defaults_fix_the_write_mode(self, tmp_path: Path) -> None:
        """The check sees the cascaded thread, as the engine runs it."""
        root = _project(
            tmp_path,
            {
                "threads/t.thread": _CDC_OVERWRITE,
                "w.weave": (
                    'config_version: "1.0"\ndefaults:\n  write: {mode: merge, match_keys: [id]}\n'
                    "threads:\n  - ref: threads/t.thread\n"
                ),
            },
        )
        load_resolved(root / "w.weave", project_root=root)

    @pytest.mark.parametrize("mode", ["execute", "plan", "preview"])
    def test_every_context_mode_fails_at_load(self, mode: str, tmp_path: Path) -> None:
        """Execute used to raise ExecutionError at thread start; now every mode fails at load."""
        from unittest.mock import MagicMock

        from pyspark.sql import SparkSession

        from weevr.context import Context

        root = _project(tmp_path, {"t.thread": _CDC_OVERWRITE})
        spark = MagicMock(spec=SparkSession)
        with pytest.raises(ConfigError, match=r"thread 't'.*cdc mode"):
            Context(spark=spark, project=str(root)).run("t.thread", mode=mode)
        spark.read.assert_not_called()

    def test_validate_reports_it(self, tmp_path: Path) -> None:
        from unittest.mock import MagicMock

        from pyspark.sql import SparkSession

        from weevr.context import Context

        root = _project(tmp_path, {"t.thread": _CDC_OVERWRITE})
        result = Context(spark=MagicMock(spec=SparkSession), project=str(root)).run(
            "t.thread", mode="validate"
        )
        assert result.status == "failure"
        assert [(d.code, d.file) for d in result.diagnostics] == [("C480", str(root / "t.thread"))]


class TestIncrementalParameterWarning:
    """The "no ${param.*}" warning reads the thread as written, before substitution."""

    _MESSAGE = "incremental_parameter mode but no ${param.*} references"

    def test_param_using_thread_does_not_warn(self, tmp_path: Path) -> None:
        root = _project(tmp_path, {"t.thread": _PARAM_THREAD})
        loaded = load_resolved(root / "t.thread", project_root=root)
        assert not any(self._MESSAGE in w for w in render_diagnostics(loaded.diagnostics))

    def test_param_free_thread_warns(self, tmp_path: Path) -> None:
        root = _project(tmp_path, {"t.thread": _PARAM_FREE_THREAD})
        loaded = load_resolved(root / "t.thread", project_root=root)
        assert [w for w in render_diagnostics(loaded.diagnostics) if self._MESSAGE in w] == [
            f"WARN: thread 't': {self._MESSAGE} found in steps or sources"
        ]

    def test_param_using_template_child_does_not_warn(self, tmp_path: Path) -> None:
        root = _project(
            tmp_path,
            {
                "threads/t.thread": _PARAM_THREAD,
                "w.weave": (
                    'config_version: "1.0"\nthreads:\n'
                    "  - ref: threads/t.thread\n    as: a\n    params: {start: '2025-01-01'}\n"
                ),
            },
        )
        loaded = load_resolved(root / "w.weave", project_root=root)
        assert not any(self._MESSAGE in w for w in render_diagnostics(loaded.diagnostics))

    def test_load_config_logs_the_warning(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        import logging

        root = _project(tmp_path, {"t.thread": _PARAM_FREE_THREAD})
        with caplog.at_level(logging.WARNING, logger="weevr.config"):
            load_config(root / "t.thread", project_root=root)
        assert any(self._MESSAGE in r.getMessage() for r in caplog.records)


class TestIncrementalDiagnostics:
    """The load and the engine build the check's input the same way."""

    @staticmethod
    def _thread(**fields: object):
        from weevr.model.thread import Thread

        return Thread.model_validate(
            {
                "name": "t",
                "config_version": "1.0",
                "sources": {"src": {"type": "delta", "alias": "raw.orders"}},
                "target": {"alias": "silver.orders"},
                **fields,
            }
        )

    def test_errors_come_from_the_hydrated_thread(self) -> None:
        from weevr.config.validation import incremental_diagnostics

        load = {"mode": "incremental_watermark", "watermark_column": "ts", "watermark_type": "long"}
        thread = self._thread(load=load)
        assert incremental_diagnostics(thread) == [
            "ERROR: incremental_watermark mode requires write.mode='append' or 'merge', "
            "got 'overwrite' (the default)"
        ]

    def test_param_rule_reads_the_authored_dict(self) -> None:
        from weevr.config.validation import incremental_diagnostics

        thread = self._thread(load={"mode": "incremental_parameter"}, write={"mode": "append"})
        authored = {"steps": [{"filter": {"expr": "d > '${param.start}'"}}]}
        assert incremental_diagnostics(thread, authored) == []
        assert any("incremental_parameter" in d for d in incremental_diagnostics(thread))
