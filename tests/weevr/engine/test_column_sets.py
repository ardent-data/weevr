"""Tests for column set resolution engine."""

from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import SparkSession

from documents import write_document, write_project
from weevr.engine.column_sets import materialize_column_sets, resolve_column_set
from weevr.errors.exceptions import ConfigError, ExecutionError
from weevr.model.column_set import ColumnSet, ColumnSetSource
from weevr.model.connection import OneLakeConnection
from weevr.telemetry.collector import SpanCollector
from weevr.telemetry.span import generate_trace_id


def _make_column_set(
    *,
    cs_type: str = "delta",
    alias: str | None = "db.col_map",
    path: str | None = None,
    from_column: str = "source_name",
    to_column: str = "target_name",
    cs_filter: str | None = None,
    param: str | None = None,
    on_failure: str = "abort",
) -> ColumnSet:
    """Build a ColumnSet for testing."""
    if param is not None:
        return ColumnSet(param=param, on_failure=on_failure)  # type: ignore[arg-type]
    source = ColumnSetSource(
        type=cs_type,  # type: ignore[arg-type]
        alias=alias,
        path=path,
        from_column=from_column,
        to_column=to_column,
        filter=cs_filter,
    )
    return ColumnSet(source=source, on_failure=on_failure)  # type: ignore[arg-type]


def _make_mock_df(
    rows: list[tuple[str, str]],
    from_col: str = "source_name",
    to_col: str = "target_name",
) -> MagicMock:
    """Build a mock DataFrame whose .collect() returns Row-like objects."""
    mock_df = MagicMock()
    mock_rows = []
    for from_val, to_val in rows:
        row = MagicMock()
        row.__getitem__ = lambda self, key, _f=from_col, _t=to_col, _fv=from_val, _tv=to_val: (
            _fv if key == _f else _tv
        )
        mock_rows.append(row)
    mock_df.collect.return_value = mock_rows
    return mock_df


class TestResolveColumnSetDeltaSource:
    """Tests for resolve_column_set with Delta source."""

    @patch("weevr.engine.column_sets.read_source")
    def test_delta_source_returns_dict(self, mock_read):
        """Delta source: select from/to columns and return dict."""
        selected_df = MagicMock()
        row_a = MagicMock()
        row_a.__getitem__ = lambda self, key: "col_a" if key == "source_name" else "renamed_a"
        row_b = MagicMock()
        row_b.__getitem__ = lambda self, key: "col_b" if key == "source_name" else "renamed_b"
        selected_df.collect.return_value = [row_a, row_b]

        raw_df = MagicMock()
        raw_df.filter.return_value = raw_df
        raw_df.select.return_value = selected_df
        mock_read.return_value = raw_df

        spark = MagicMock()
        cs = _make_column_set()

        result = resolve_column_set(spark, "my_set", cs, {})

        assert result == {"col_a": "renamed_a", "col_b": "renamed_b"}
        mock_read.assert_called_once()

    @patch("weevr.engine.column_sets.F.expr")
    @patch("weevr.engine.column_sets.read_source")
    def test_alias_path_filter_and_custom_columns_after_branch_split(self, mock_read, mock_expr):
        """Regression guard: alias-backed column sets still honour filter and
        custom from_column/to_column after the connection/alias branch split
        in _resolve_from_source. Combines the three dimensions in one test so
        the else-branch can never silently regress."""
        mock_expr.return_value = "mock_filter_col"

        filtered_df = MagicMock()
        selected_df = MagicMock()
        row = MagicMock()
        row.__getitem__ = lambda self, key: "raw_a" if key == "src_col" else "friendly_a"
        selected_df.collect.return_value = [row]

        raw_df = MagicMock()
        raw_df.filter.return_value = filtered_df
        filtered_df.select.return_value = selected_df
        mock_read.return_value = raw_df

        spark = MagicMock()
        cs = ColumnSet(
            source=ColumnSetSource(
                type="delta",
                alias="silver.col_map",
                from_column="src_col",
                to_column="tgt_col",
                filter="dataset = 'orders'",
            )
        )

        result = resolve_column_set(spark, "cs", cs, {})

        assert result == {"raw_a": "friendly_a"}
        # Alias path built an alias-based Source (not connection-based)
        passed_source = mock_read.call_args.args[2]
        assert passed_source.alias == "silver.col_map"
        assert passed_source.connection is None
        # Filter compiled and applied before select
        mock_expr.assert_called_once_with("dataset = 'orders'")
        raw_df.filter.assert_called_once_with("mock_filter_col")
        # Custom column names reach the select call
        filtered_df.select.assert_called_once_with("src_col", "tgt_col")

    @patch("weevr.engine.column_sets.read_source")
    def test_custom_column_names(self, mock_read):
        """Custom from_column / to_column names are respected."""
        selected_df = MagicMock()
        row = MagicMock()
        row.__getitem__ = lambda self, key: "old" if key == "from_col" else "new"
        selected_df.collect.return_value = [row]

        raw_df = MagicMock()
        raw_df.filter.return_value = raw_df
        raw_df.select.return_value = selected_df
        mock_read.return_value = raw_df

        spark = MagicMock()
        cs = _make_column_set(from_column="from_col", to_column="to_col")

        result = resolve_column_set(spark, "cs", cs, {})

        assert result == {"old": "new"}
        raw_df.select.assert_called_once_with("from_col", "to_col")


class TestResolveColumnSetConnectionSource:
    """Tests for resolve_column_set with a connection-backed Delta source."""

    def _connection(self) -> OneLakeConnection:
        return OneLakeConnection(
            type="onelake",
            workspace="ws-guid",
            lakehouse="lh-guid",
        )

    @patch("weevr.engine.column_sets.read_source")
    def test_connection_forwarded_to_read_source(self, mock_read):
        """Connection-backed column set passes connections dict to read_source."""
        selected_df = MagicMock()
        row = MagicMock()
        row.__getitem__ = lambda self, key: "src" if key == "source_name" else "tgt"
        selected_df.collect.return_value = [row]

        raw_df = MagicMock()
        raw_df.select.return_value = selected_df
        mock_read.return_value = raw_df

        spark = MagicMock()
        connections = {"ref": self._connection()}
        cs = ColumnSet(source=ColumnSetSource(type="delta", connection="ref", table="rename_map"))

        result = resolve_column_set(spark, "my_cs", cs, {}, connections=connections)

        assert result == {"src": "tgt"}
        # Verify read_source got the connections kwarg with our dict
        mock_read.assert_called_once()
        call_kwargs = mock_read.call_args.kwargs
        assert call_kwargs.get("connections") is connections
        # Verify the inner Source was built with connection + table (not alias)
        passed_source = mock_read.call_args.args[2]
        assert passed_source.connection == "ref"
        assert passed_source.table == "rename_map"
        assert passed_source.alias is None

    @patch("weevr.engine.column_sets.read_source")
    def test_schema_override_forwarded(self, mock_read):
        """schema field on the column set source is forwarded as schema_override."""
        selected_df = MagicMock()
        row = MagicMock()
        row.__getitem__ = lambda self, key: "a" if key == "source_name" else "b"
        selected_df.collect.return_value = [row]

        raw_df = MagicMock()
        raw_df.select.return_value = selected_df
        mock_read.return_value = raw_df

        spark = MagicMock()
        connections = {"ref": self._connection()}
        cs = ColumnSet(
            source=ColumnSetSource.model_validate(
                {
                    "type": "delta",
                    "connection": "ref",
                    "schema": "dictionary",
                    "table": "rename_map",
                }
            )
        )

        resolve_column_set(spark, "my_cs", cs, {}, connections=connections)

        passed_source = mock_read.call_args.args[2]
        assert passed_source.connection == "ref"
        assert passed_source.schema_override == "dictionary"
        assert passed_source.table == "rename_map"

    @patch("weevr.engine.column_sets.read_source")
    def test_missing_connection_wraps_error_with_column_set_name(self, mock_read):
        """When read_source raises ExecutionError (e.g. undefined connection),
        the error is re-raised with the column set name prepended so that
        users with many column sets can identify which one failed."""
        mock_read.side_effect = ExecutionError(
            "Source references undefined connection 'ref'",
            source_name="ref",
        )
        spark = MagicMock()
        cs = ColumnSet(
            source=ColumnSetSource(type="delta", connection="ref", table="rename_map"),
            on_failure="abort",
        )

        with pytest.raises(ExecutionError) as exc_info:
            resolve_column_set(spark, "my_cs", cs, {}, connections={})

        # Column set name must appear in the surface message
        assert "my_cs" in str(exc_info.value)
        # Underlying connection error must still be visible
        assert "undefined connection 'ref'" in str(exc_info.value)
        # Chain preserved for debugging
        assert isinstance(exc_info.value.__cause__, ExecutionError)

    @patch("weevr.engine.column_sets.read_source")
    def test_execution_error_honours_on_failure_warn(self, mock_read, caplog):
        """ExecutionError from read_source with on_failure='warn' returns an
        empty dict and logs the wrapped message."""
        import logging

        mock_read.side_effect = ExecutionError(
            "Source references undefined connection 'ref'",
            source_name="ref",
        )
        spark = MagicMock()
        cs = ColumnSet(
            source=ColumnSetSource(type="delta", connection="ref", table="rename_map"),
            on_failure="warn",
        )

        with caplog.at_level(logging.WARNING, logger="weevr.engine.column_sets"):
            result = resolve_column_set(spark, "my_cs", cs, {}, connections={})

        assert result == {}
        assert any("my_cs" in r.message for r in caplog.records)

    @patch("weevr.engine.column_sets.read_source")
    def test_alias_path_does_not_pass_connection_fields(self, mock_read):
        """Alias-backed column sets continue to build a Source without connection."""
        selected_df = MagicMock()
        row = MagicMock()
        row.__getitem__ = lambda self, key: "a" if key == "source_name" else "b"
        selected_df.collect.return_value = [row]

        raw_df = MagicMock()
        raw_df.select.return_value = selected_df
        mock_read.return_value = raw_df

        spark = MagicMock()
        cs = ColumnSet(source=ColumnSetSource(type="delta", alias="silver.col_map"))

        resolve_column_set(spark, "my_cs", cs, {})

        passed_source = mock_read.call_args.args[2]
        assert passed_source.alias == "silver.col_map"
        assert passed_source.connection is None
        assert passed_source.table is None


class TestMaterializeColumnSetsWithConnections:
    """Tests for materialize_column_sets connections forwarding."""

    @patch("weevr.engine.column_sets.read_source")
    def test_connections_forwarded_to_each_resolve(self, mock_read):
        """The connections dict reaches every column set's read_source call."""
        selected_df = MagicMock()
        row = MagicMock()
        row.__getitem__ = lambda self, key: "a" if key == "source_name" else "b"
        selected_df.collect.return_value = [row]

        raw_df = MagicMock()
        raw_df.select.return_value = selected_df
        mock_read.return_value = raw_df

        spark = MagicMock()
        connections = {
            "ref": OneLakeConnection(type="onelake", workspace="ws", lakehouse="lh"),
        }
        column_sets = {
            "cs_a": ColumnSet(
                source=ColumnSetSource(type="delta", connection="ref", table="map_a")
            ),
            "cs_b": ColumnSet(
                source=ColumnSetSource(type="delta", connection="ref", table="map_b")
            ),
        }

        resolved, _ = materialize_column_sets(spark, column_sets, {}, connections=connections)

        assert "cs_a" in resolved and "cs_b" in resolved
        assert mock_read.call_count == 2
        for call in mock_read.call_args_list:
            assert call.kwargs.get("connections") is connections


class TestResolveColumnSetParamSource:
    """Tests for resolve_column_set with param source."""

    def test_param_source_returns_dict(self):
        """Param source: read from resolved_params directly."""
        cs = _make_column_set(param="my_dict")
        result = resolve_column_set(MagicMock(), "cs", cs, {"my_dict": {"A": "a", "B": "b"}})
        assert result == {"A": "a", "B": "b"}

    def test_param_not_a_dict_raises(self):
        """Param value that is not a dict raises ConfigError."""
        cs = _make_column_set(param="bad_param")
        with pytest.raises(ConfigError, match="must be a dict"):
            resolve_column_set(MagicMock(), "cs", cs, {"bad_param": ["A", "B"]})

    def test_param_missing_raises(self):
        """Missing param key raises ConfigError."""
        cs = _make_column_set(param="missing")
        with pytest.raises(ConfigError, match="param 'missing' not found"):
            resolve_column_set(MagicMock(), "cs", cs, {})


class TestResolveColumnSetFilter:
    """Tests for filter expression application."""

    @patch("weevr.engine.column_sets.F.expr")
    @patch("weevr.engine.column_sets.read_source")
    def test_filter_applied_before_collect(self, mock_read, mock_expr):
        """Filter expression is applied to the DataFrame before collect."""
        mock_expr.return_value = "mock_filter_col"

        filtered_df = MagicMock()
        selected_df = MagicMock()
        row = MagicMock()
        row.__getitem__ = lambda self, key: "a" if key == "source_name" else "b"
        selected_df.collect.return_value = [row]

        raw_df = MagicMock()
        raw_df.filter.return_value = filtered_df
        filtered_df.select.return_value = selected_df
        mock_read.return_value = raw_df

        spark = MagicMock()
        cs = _make_column_set(cs_filter="dataset = 'orders'")

        resolve_column_set(spark, "cs", cs, {})

        mock_expr.assert_called_once_with("dataset = 'orders'")
        raw_df.filter.assert_called_once_with("mock_filter_col")
        filtered_df.select.assert_called_once()

    @patch("weevr.engine.column_sets.read_source")
    def test_no_filter_skips_filter_call(self, mock_read):
        """When no filter is set, filter() is not called on the DataFrame."""
        selected_df = MagicMock()
        row = MagicMock()
        row.__getitem__ = lambda self, key: "a" if key == "source_name" else "b"
        selected_df.collect.return_value = [row]

        raw_df = MagicMock()
        raw_df.select.return_value = selected_df
        mock_read.return_value = raw_df

        spark = MagicMock()
        cs = _make_column_set()  # no filter

        resolve_column_set(spark, "cs", cs, {})

        raw_df.filter.assert_not_called()


class TestResolveColumnSetOnFailure:
    """Tests for on_failure handling."""

    @patch("weevr.engine.column_sets.read_source")
    def test_empty_source_abort_raises(self, mock_read):
        """Empty source with on_failure=abort raises ExecutionError."""
        selected_df = MagicMock()
        selected_df.collect.return_value = []

        raw_df = MagicMock()
        raw_df.select.return_value = selected_df
        mock_read.return_value = raw_df

        spark = MagicMock()
        cs = _make_column_set(on_failure="abort")

        with pytest.raises(ExecutionError, match="empty"):
            resolve_column_set(spark, "my_set", cs, {})

    @patch("weevr.engine.column_sets.read_source")
    def test_empty_source_warn_returns_empty_dict(self, mock_read, caplog):
        """Empty source with on_failure=warn returns empty dict and logs warning."""
        import logging

        selected_df = MagicMock()
        selected_df.collect.return_value = []

        raw_df = MagicMock()
        raw_df.select.return_value = selected_df
        mock_read.return_value = raw_df

        spark = MagicMock()
        cs = _make_column_set(on_failure="warn")

        with caplog.at_level(logging.WARNING, logger="weevr.engine.column_sets"):
            result = resolve_column_set(spark, "my_set", cs, {})

        assert result == {}
        assert any("empty" in r.message.lower() for r in caplog.records)

    @patch("weevr.engine.column_sets.read_source")
    def test_empty_source_skip_returns_none(self, mock_read):
        """Empty source with on_failure=skip returns None."""
        selected_df = MagicMock()
        selected_df.collect.return_value = []

        raw_df = MagicMock()
        raw_df.select.return_value = selected_df
        mock_read.return_value = raw_df

        spark = MagicMock()
        cs = _make_column_set(on_failure="skip")

        result = resolve_column_set(spark, "my_set", cs, {})

        assert result is None

    @patch("weevr.engine.column_sets.read_source")
    def test_read_failure_abort_raises(self, mock_read):
        """Read error with on_failure=abort raises ExecutionError."""
        mock_read.side_effect = RuntimeError("table not found")
        spark = MagicMock()
        cs = _make_column_set(on_failure="abort")

        with pytest.raises(ExecutionError, match="Failed to resolve column set"):
            resolve_column_set(spark, "my_set", cs, {})

    @patch("weevr.engine.column_sets.read_source")
    def test_read_failure_warn_returns_empty_dict(self, mock_read, caplog):
        """Read error with on_failure=warn returns empty dict and logs warning."""
        import logging

        mock_read.side_effect = RuntimeError("table not found")
        spark = MagicMock()
        cs = _make_column_set(on_failure="warn")

        with caplog.at_level(logging.WARNING, logger="weevr.engine.column_sets"):
            result = resolve_column_set(spark, "my_set", cs, {})

        assert result == {}
        assert len(caplog.records) > 0

    @patch("weevr.engine.column_sets.read_source")
    def test_read_failure_skip_returns_none(self, mock_read):
        """Read error with on_failure=skip returns None."""
        mock_read.side_effect = RuntimeError("table not found")
        spark = MagicMock()
        cs = _make_column_set(on_failure="skip")

        result = resolve_column_set(spark, "my_set", cs, {})

        assert result is None


class TestDuplicateFromColumnValidation:
    """Tests for duplicate from_column invariant."""

    @patch("weevr.engine.column_sets.read_source")
    def test_duplicate_from_column_raises(self, mock_read):
        """Duplicate from_column values in source raise ConfigError."""
        selected_df = MagicMock()
        row_a = MagicMock()
        row_a.__getitem__ = lambda self, key: "col_a" if key == "source_name" else "renamed_a"
        row_dup = MagicMock()
        row_dup.__getitem__ = lambda self, key: "col_a" if key == "source_name" else "renamed_b"
        selected_df.collect.return_value = [row_a, row_dup]

        raw_df = MagicMock()
        raw_df.select.return_value = selected_df
        mock_read.return_value = raw_df

        spark = MagicMock()
        cs = _make_column_set()

        with pytest.raises(ConfigError, match="duplicate"):
            resolve_column_set(spark, "my_set", cs, {})


class TestMaterializeColumnSets:
    """Tests for materialize_column_sets."""

    @patch("weevr.engine.column_sets.read_source")
    def test_materializes_all_column_sets(self, mock_read):
        """All column sets are resolved and returned in a mapping."""
        selected_df = MagicMock()
        row = MagicMock()
        row.__getitem__ = lambda self, key: "a" if key == "source_name" else "b"
        selected_df.collect.return_value = [row]

        raw_df = MagicMock()
        raw_df.select.return_value = selected_df
        mock_read.return_value = raw_df

        spark = MagicMock()
        column_sets = {
            "cs1": _make_column_set(),
            "cs2": _make_column_set(alias="db.other_map"),
        }

        resolved, results = materialize_column_sets(spark, column_sets, {})

        assert "cs1" in resolved
        assert "cs2" in resolved
        assert resolved["cs1"] == {"a": "b"}
        assert resolved["cs2"] == {"a": "b"}
        assert len(results) == 2

    def test_param_column_sets_resolved(self):
        """Param-sourced column sets are resolved from resolved_params."""
        spark = MagicMock()
        column_sets = {
            "rename_map": _make_column_set(param="my_map"),
        }
        resolved_params = {"my_map": {"col_x": "col_y"}}

        resolved, results = materialize_column_sets(spark, column_sets, resolved_params)

        assert resolved == {"rename_map": {"col_x": "col_y"}}
        assert len(results) == 1
        assert results[0].name == "rename_map"
        assert results[0].source_type == "param"
        assert results[0].mappings_loaded == 1

    @patch("weevr.engine.column_sets.read_source")
    def test_skip_none_results(self, mock_read):
        """Column sets returning None (on_failure=skip) are excluded from result."""
        selected_df = MagicMock()
        selected_df.collect.return_value = []  # empty → skip

        raw_df = MagicMock()
        raw_df.select.return_value = selected_df
        mock_read.return_value = raw_df

        spark = MagicMock()
        column_sets = {
            "skip_me": _make_column_set(on_failure="skip"),
        }

        resolved, results = materialize_column_sets(spark, column_sets, {})

        assert "skip_me" not in resolved
        assert len(results) == 1
        assert results[0].skipped is True

    @patch("weevr.engine.column_sets.read_source")
    def test_telemetry_spans_created(self, mock_read):
        """Telemetry spans are created for each resolved column set."""
        selected_df = MagicMock()
        row = MagicMock()
        row.__getitem__ = lambda self, key: "a" if key == "source_name" else "b"
        selected_df.collect.return_value = [row]

        raw_df = MagicMock()
        raw_df.select.return_value = selected_df
        mock_read.return_value = raw_df

        spark = MagicMock()
        collector = SpanCollector(generate_trace_id())
        column_sets = {"my_cs": _make_column_set()}

        materialize_column_sets(spark, column_sets, {}, collector=collector, parent_span_id="p1")

        spans = collector.get_spans()
        assert len(spans) == 1
        assert spans[0].name == "column_set:resolve:my_cs"
        assert spans[0].parent_span_id == "p1"

    @patch("weevr.engine.column_sets.read_source")
    def test_span_attributes_include_source_type_and_mappings_loaded(self, mock_read):
        """Span attributes include column_set.name, source_type, and mappings_loaded."""
        selected_df = MagicMock()
        row_a = MagicMock()
        row_a.__getitem__ = lambda self, key: "col_a" if key == "source_name" else "col_b"
        row_b = MagicMock()
        row_b.__getitem__ = lambda self, key: "col_c" if key == "source_name" else "col_d"
        selected_df.collect.return_value = [row_a, row_b]

        raw_df = MagicMock()
        raw_df.select.return_value = selected_df
        mock_read.return_value = raw_df

        spark = MagicMock()
        collector = SpanCollector(generate_trace_id())
        column_sets = {"my_delta_cs": _make_column_set(cs_type="delta")}

        materialize_column_sets(spark, column_sets, {}, collector=collector)

        spans = collector.get_spans()
        assert len(spans) == 1
        attrs = spans[0].attributes
        assert attrs.get("column_set.name") == "my_delta_cs"
        assert attrs.get("column_set.source_type") == "delta"
        assert attrs.get("column_set.mappings_loaded") == 2

    @patch("weevr.engine.column_sets.read_source")
    def test_span_attributes_param_source_type(self, mock_read):
        """Param-sourced column set span has source_type='param'."""
        spark = MagicMock()
        collector = SpanCollector(generate_trace_id())
        column_sets = {"param_cs": _make_column_set(param="my_map")}
        resolved_params = {"my_map": {"a": "b", "c": "d"}}

        materialize_column_sets(spark, column_sets, resolved_params, collector=collector)

        spans = collector.get_spans()
        assert len(spans) == 1
        attrs = spans[0].attributes
        assert attrs.get("column_set.source_type") == "param"
        assert attrs.get("column_set.mappings_loaded") == 2

    @patch("weevr.engine.column_sets.read_source")
    def test_empty_column_sets_returns_empty_dict(self, mock_read):
        """No column sets defined returns empty result."""
        resolved, results = materialize_column_sets(MagicMock(), {}, {})
        assert resolved == {}
        assert results == []
        mock_read.assert_not_called()


@pytest.mark.spark
class TestResolveOncePerScope:
    """Column sets resolve once per declaring scope; precedence unchanged."""

    @pytest.fixture()
    def materialize_spy(self, monkeypatch: pytest.MonkeyPatch) -> list[list[str]]:
        """Record the def-name sets each materialization call resolves."""
        from weevr.engine import column_sets as column_sets_mod
        from weevr.engine import runner as runner_mod

        calls: list[list[str]] = []
        original = column_sets_mod.materialize_column_sets

        def _spy(spark_arg, defs, *args, **kwargs):  # type: ignore[no-untyped-def]
            calls.append(sorted(defs))
            return original(spark_arg, defs, *args, **kwargs)

        monkeypatch.setattr(runner_mod, "materialize_column_sets", _spy)
        return calls

    def test_loom_set_resolves_once_across_weaves(
        self, spark: SparkSession, tmp_path, materialize_spy: list[list[str]]
    ) -> None:
        from weevr.engine.runner import execute_loom
        from weevr.model.loom import Loom
        from weevr.model.thread import Thread
        from weevr.model.weave import Weave

        src = str(tmp_path / "rps_src")
        spark.createDataFrame([{"from_a": 1}]).write.format("delta").save(src)

        def _weave(name: str, tgt: str) -> Weave:
            return Weave.model_validate(
                {
                    "name": name,
                    "config_version": "2.0",
                    "threads": [{"name": f"{name}_t"}],
                }
            )

        def _thread(name: str, tgt: str) -> Thread:
            return Thread.model_validate(
                {
                    "name": name,
                    "config_version": "2.0",
                    "sources": {"main": {"type": "delta", "alias": src}},
                    "steps": [{"rename": {"column_set": "loomset"}}],
                    "target": {"path": tgt},
                    "write": {"mode": "overwrite"},
                }
            )

        loom = Loom.model_validate(
            {
                "name": "rps_loom",
                "config_version": "2.0",
                "weaves": [{"name": "w1"}, {"name": "w2"}],
                "column_sets": {"loomset": {"param": "cs_param"}},
            }
        )
        weaves = {
            "w1": _weave("w1", str(tmp_path / "t1")),
            "w2": _weave("w2", str(tmp_path / "t2")),
        }
        threads = {
            "w1": {"w1_t": _thread("w1_t", str(tmp_path / "t1"))},
            "w2": {"w2_t": _thread("w2_t", str(tmp_path / "t2"))},
        }
        result = execute_loom(
            spark,
            loom,
            weaves,
            threads,
            params={"cs_param": {"from_a": "to_a"}},
        )
        assert result.status == "success"
        # The loom set materialized exactly once, at loom scope — neither
        # weave re-resolved it
        assert materialize_spy == [["loomset"]]
        # Params-invariance: both weaves observed the SAME loom-resolved
        # mapping (params flow as one unchanged object loom→weave→thread)
        cols1 = spark.read.format("delta").load(str(tmp_path / "t1")).columns
        cols2 = spark.read.format("delta").load(str(tmp_path / "t2")).columns
        assert "to_a" in cols1 and "to_a" in cols2

    def test_weave_override_still_wins(
        self, spark: SparkSession, tmp_path, materialize_spy: list[list[str]]
    ) -> None:
        from weevr.engine.planner import build_plan
        from weevr.engine.runner import execute_weave
        from weevr.model.thread import Thread
        from weevr.model.weave import ThreadEntry

        src = str(tmp_path / "rps_ov_src")
        tgt = str(tmp_path / "rps_ov_tgt")
        spark.createDataFrame([{"src_col": 1}]).write.format("delta").save(src)

        thread = Thread.model_validate(
            {
                "name": "ov_t",
                "config_version": "2.0",
                "sources": {"main": {"type": "delta", "alias": src}},
                "steps": [{"rename": {"column_set": "shared"}}],
                "target": {"path": tgt},
                "write": {"mode": "overwrite"},
            }
        )
        threads = {"ov_t": thread}
        plan = build_plan("rps_ov", threads, [ThreadEntry(name="ov_t")])

        from weevr.model.column_set import ColumnSet

        weave_def = ColumnSet(param="weave_map")  # type: ignore[arg-type]
        # Loom pre-resolved a DIFFERENT mapping under the same name; the
        # caller (execute_loom) excludes overridden keys, so here the weave
        # def must be the one that materializes
        result = execute_weave(
            spark,
            plan,
            threads,
            column_set_defs={"shared": weave_def},
            params={"weave_map": {"src_col": "renamed_by_weave"}},
            pre_resolved_column_sets=None,
            pre_column_set_results=None,
        )
        assert result.status == "success"
        out_cols = spark.read.format("delta").load(tgt).columns
        assert "renamed_by_weave" in out_cols  # weave mapping applied
        assert materialize_spy == [["shared"]]

    def test_pre_resolved_sets_skip_weave_materialization(
        self, spark: SparkSession, tmp_path, materialize_spy: list[list[str]]
    ) -> None:
        from weevr.engine.planner import build_plan
        from weevr.engine.runner import execute_weave
        from weevr.model.column_set import ColumnSet
        from weevr.model.thread import Thread
        from weevr.model.weave import ThreadEntry

        src = str(tmp_path / "rps_pre_src")
        tgt = str(tmp_path / "rps_pre_tgt")
        spark.createDataFrame([{"src_col": 1}]).write.format("delta").save(src)

        thread = Thread.model_validate(
            {
                "name": "pre_t",
                "config_version": "2.0",
                "sources": {"main": {"type": "delta", "alias": src}},
                "steps": [{"rename": {"column_set": "loomset"}}],
                "target": {"path": tgt},
                "write": {"mode": "overwrite"},
            }
        )
        threads = {"pre_t": thread}
        plan = build_plan("rps_pre", threads, [ThreadEntry(name="pre_t")])

        loom_def = ColumnSet(param="unused")  # type: ignore[arg-type]
        result = execute_weave(
            spark,
            plan,
            threads,
            column_set_defs={"loomset": loom_def},
            pre_resolved_column_sets={"loomset": {"src_col": "renamed_by_loom"}},
        )
        assert result.status == "success"
        out_cols = spark.read.format("delta").load(tgt).columns
        assert "renamed_by_loom" in out_cols  # pre-resolved mapping served
        assert materialize_spy == []  # nothing re-resolved at weave scope


@pytest.mark.spark
class TestYamlColumnSetFile:
    """YAML column sets read their mapping from the file."""

    @staticmethod
    def _write(tmp_path, text: str, name: str = "cols.yaml") -> str:
        path = tmp_path / name
        path.write_text(text)
        return str(path)

    def test_rows_become_the_mapping(self, spark: SparkSession, tmp_path) -> None:
        path = self._write(
            tmp_path,
            "- source_name: cust_id\n  target_name: customer_id\n"
            "- source_name: cust_nm\n  target_name: customer_name\n",
        )
        cs = _make_column_set(cs_type="yaml", alias=None, path=path)

        with patch("weevr.engine.column_sets.read_source") as mock_read:
            result = resolve_column_set(spark, "yaml_set", cs, {})

        assert result == {"cust_id": "customer_id", "cust_nm": "customer_name"}
        mock_read.assert_not_called()

    def test_custom_from_and_to_columns(self, spark: SparkSession, tmp_path) -> None:
        path = self._write(tmp_path, "- raw: a\n  friendly: alpha\n  note: ignored\n")
        cs = _make_column_set(
            cs_type="yaml", alias=None, path=path, from_column="raw", to_column="friendly"
        )

        assert resolve_column_set(spark, "yaml_set", cs, {}) == {"a": "alpha"}

    def test_filter_compares_string_values(self, spark: SparkSession, tmp_path) -> None:
        """Rows missing a key get null for it; values are compared as strings."""
        path = self._write(
            tmp_path,
            "- source_name: a\n  target_name: alpha\n  level: 2\n"
            "- source_name: b\n  target_name: beta\n  level: 3\n"
            "- source_name: c\n  target_name: gamma\n",
        )
        cs = _make_column_set(cs_type="yaml", alias=None, path=path, cs_filter="level = '2'")

        assert resolve_column_set(spark, "yaml_set", cs, {}) == {"a": "alpha"}

    def test_non_string_scalars_are_stringified(self, spark: SparkSession, tmp_path) -> None:
        path = self._write(tmp_path, "- source_name: 1\n  target_name: true\n")
        cs = _make_column_set(cs_type="yaml", alias=None, path=path)

        assert resolve_column_set(spark, "yaml_set", cs, {}) == {"1": "True"}

    @pytest.mark.parametrize("text", ["", "[]\n"])
    @pytest.mark.parametrize(("on_failure", "expected"), [("warn", {}), ("skip", None)])
    def test_empty_file_is_no_rows(
        self, spark: SparkSession, tmp_path, text: str, on_failure: str, expected
    ) -> None:
        path = self._write(tmp_path, text)
        cs = _make_column_set(cs_type="yaml", alias=None, path=path, on_failure=on_failure)

        assert resolve_column_set(spark, "yaml_set", cs, {}) == expected

    def test_empty_file_aborts_as_no_rows(self, spark: SparkSession, tmp_path) -> None:
        path = self._write(tmp_path, "[]\n")
        cs = _make_column_set(cs_type="yaml", alias=None, path=path)

        with pytest.raises(ExecutionError, match="0 rows"):
            resolve_column_set(spark, "yaml_set", cs, {})

    @pytest.mark.parametrize(
        "text",
        [
            "source_name: a\ntarget_name: b\n",  # a mapping, not a list
            "- just a string\n",  # a row that isn't a mapping
            "- source_name: a\n",  # a row without the to-column
            "- [unclosed\n",  # not valid YAML
        ],
    )
    @pytest.mark.parametrize(("on_failure", "expected"), [("warn", {}), ("skip", None)])
    def test_unreadable_file_honours_on_failure(
        self, spark: SparkSession, tmp_path, text: str, on_failure: str, expected
    ) -> None:
        path = self._write(tmp_path, text)
        cs = _make_column_set(cs_type="yaml", alias=None, path=path, on_failure=on_failure)

        assert resolve_column_set(spark, "yaml_set", cs, {}) == expected

    @pytest.mark.parametrize(
        "text", ["source_name: a\ntarget_name: b\n", "- source_name: a\n", "- [unclosed\n"]
    )
    def test_unreadable_file_aborts_naming_the_set(
        self, spark: SparkSession, tmp_path, text: str
    ) -> None:
        path = self._write(tmp_path, text)
        cs = _make_column_set(cs_type="yaml", alias=None, path=path)

        with pytest.raises(ExecutionError, match="yaml_set"):
            resolve_column_set(spark, "yaml_set", cs, {})

    def test_unquoted_bool_like_key_error_says_to_quote(
        self, spark: SparkSession, tmp_path
    ) -> None:
        """YAML 1.1 reads an unquoted `on:` key as a boolean; the error says to quote it."""
        path = self._write(tmp_path, "- source_name: a\n  on: b\n")
        cs = _make_column_set(cs_type="yaml", alias=None, path=path, to_column="on")

        with pytest.raises(ExecutionError, match="in quotes"):
            resolve_column_set(spark, "yaml_set", cs, {})

    def test_keys_that_collide_as_strings_are_rejected(self, spark: SparkSession, tmp_path) -> None:
        path = self._write(tmp_path, "- source_name: a\n  target_name: b\n  1: x\n  '1': y\n")
        cs = _make_column_set(cs_type="yaml", alias=None, path=path)

        with pytest.raises(ExecutionError, match="'1'"):
            resolve_column_set(spark, "yaml_set", cs, {})

    @pytest.mark.parametrize(
        "text",
        ["- source_name: a\n  target_name: null\n", "- source_name: ~\n  target_name: b\n"],
        ids=["null-to", "null-from"],
    )
    def test_null_from_or_to_value_is_rejected(
        self, spark: SparkSession, tmp_path, text: str
    ) -> None:
        path = self._write(tmp_path, text)
        cs = _make_column_set(cs_type="yaml", alias=None, path=path)

        with pytest.raises(ExecutionError, match="null"):
            resolve_column_set(spark, "yaml_set", cs, {})

    def test_null_in_another_column_stays_null(self, spark: SparkSession, tmp_path) -> None:
        path = self._write(
            tmp_path,
            "- source_name: a\n  target_name: alpha\n  note: null\n"
            "- source_name: b\n  target_name: beta\n  note: keep\n",
        )
        cs = _make_column_set(cs_type="yaml", alias=None, path=path, cs_filter="note IS NULL")

        assert resolve_column_set(spark, "yaml_set", cs, {}) == {"a": "alpha"}

    def test_missing_file_aborts_naming_the_set(self, spark: SparkSession, tmp_path) -> None:
        cs = _make_column_set(cs_type="yaml", alias=None, path=str(tmp_path / "missing.yaml"))

        with pytest.raises(ExecutionError, match="yaml_set"):
            resolve_column_set(spark, "yaml_set", cs, {})

    def test_duplicate_from_value_raises(self, spark: SparkSession, tmp_path) -> None:
        path = self._write(
            tmp_path,
            "- source_name: a\n  target_name: x\n- source_name: a\n  target_name: y\n",
        )
        cs = _make_column_set(cs_type="yaml", alias=None, path=path)

        with pytest.raises(ConfigError, match="duplicate"):
            resolve_column_set(spark, "yaml_set", cs, {})

    def test_thread_renames_through_a_yaml_column_set(self, spark: SparkSession, tmp_path) -> None:
        from weevr.engine.executor import execute_thread
        from weevr.model.thread import Thread

        src = str(tmp_path / "yaml_cs_src")
        tgt = str(tmp_path / "yaml_cs_tgt")
        spark.createDataFrame([{"cust_id": 1, "other": 2}]).write.format("delta").save(src)
        path = self._write(tmp_path, "- source_name: cust_id\n  target_name: customer_id\n")
        thread = Thread.model_validate(
            {
                "name": "yaml_cs_thread",
                "config_version": "2.0",
                "sources": {"main": {"type": "delta", "alias": src}},
                "steps": [{"rename": {"column_set": "names"}}],
                "target": {"path": tgt},
                "write": {"mode": "overwrite"},
                "column_sets": {"names": {"source": {"type": "yaml", "path": path}}},
            }
        )

        result = execute_thread(spark, thread)

        assert result.status == "success"
        assert sorted(spark.read.format("delta").load(tgt).columns) == ["customer_id", "other"]

    def test_relative_path_from_a_project_file(self, spark: SparkSession, tmp_path) -> None:
        """A thread file's relative YAML path resolves against the project root and renames."""
        from weevr.config import load_config
        from weevr.engine.executor import execute_thread

        project = write_project(tmp_path / "cs.weevr")
        (project / "maps").mkdir(parents=True)
        (project / "maps/names.yaml").write_text(
            "- source_name: cust_id\n  target_name: customer_id\n"
        )
        src = str(tmp_path / "rel_cs_src")
        tgt = str(tmp_path / "rel_cs_tgt")
        spark.createDataFrame([{"cust_id": 1}]).write.format("delta").save(src)
        write_document(
            project / "orders.thread",
            'config_version: "2.0"\n'
            f"sources:\n  main:\n    type: delta\n    alias: {src}\n"
            "steps:\n  - rename:\n      column_set: names\n"
            f"target:\n  path: {tgt}\n"
            "write:\n  mode: overwrite\n"
            "column_sets:\n  names:\n    source:\n      type: yaml\n      path: maps/names.yaml\n",
        )

        thread = load_config(project / "orders.thread", project_root=project)
        result = execute_thread(spark, thread)  # type: ignore[arg-type]

        assert result.status == "success"
        assert spark.read.format("delta").load(tgt).columns == ["customer_id"]
