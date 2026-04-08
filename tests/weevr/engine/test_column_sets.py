"""Tests for column set resolution engine."""

from unittest.mock import MagicMock, patch

import pytest

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

    @patch("weevr.engine.column_sets.read_source")
    def test_yaml_source_returns_dict(self, mock_read):
        """YAML source: behaves the same as Delta (both go through read_source)."""
        selected_df = MagicMock()
        row = MagicMock()
        row.__getitem__ = lambda self, key: "x" if key == "source_name" else "y"
        selected_df.collect.return_value = [row]

        raw_df = MagicMock()
        raw_df.filter.return_value = raw_df
        raw_df.select.return_value = selected_df
        mock_read.return_value = raw_df

        spark = MagicMock()
        cs = _make_column_set(cs_type="yaml", alias=None, path="/some/file.yaml")

        result = resolve_column_set(spark, "yaml_set", cs, {})

        assert result == {"x": "y"}
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
