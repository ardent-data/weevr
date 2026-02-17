"""Tests for config inheritance cascade."""

import pytest

from weevr.config.inheritance import apply_inheritance, cascade


class TestCascade:
    """Test cascade function."""

    def test_scalar_override(self):
        """Child scalar value should override parent."""
        parent = {"mode": "overwrite", "table": "parent_table"}
        child = {"mode": "merge"}
        result = cascade(parent, child)
        assert result["mode"] == "merge"  # Child wins
        assert result["table"] == "parent_table"  # Parent inherited

    def test_list_replacement(self):
        """Child list should replace parent list entirely."""
        parent = {"tags": ["a", "b"]}
        child = {"tags": ["c", "d", "e"]}
        result = cascade(parent, child)
        assert result["tags"] == ["c", "d", "e"]  # Child's list entirely

    def test_dict_replacement(self):
        """Child dict should replace parent dict entirely (no deep merge)."""
        parent = {"write": {"mode": "overwrite", "format": "delta", "partition": "date"}}
        child = {"write": {"mode": "merge"}}
        result = cascade(parent, child)
        # Child's write dict entirely replaces parent's
        assert result["write"] == {"mode": "merge"}
        assert "format" not in result["write"]
        assert "partition" not in result["write"]

    def test_inheritance_from_parent(self):
        """Keys in parent but not in child should be inherited."""
        parent = {"audit_template": "standard", "tags": ["parent"]}
        child = {"mode": "merge"}
        result = cascade(parent, child)
        assert result["audit_template"] == "standard"  # Inherited
        assert result["mode"] == "merge"  # From child
        assert result["tags"] == ["parent"]  # Inherited

    def test_new_keys_in_child(self):
        """Keys in child but not in parent should be kept."""
        parent = {"mode": "overwrite"}
        child = {"mode": "merge", "new_key": "new_value"}
        result = cascade(parent, child)
        assert result["mode"] == "merge"
        assert result["new_key"] == "new_value"

    def test_internal_keys_preserved(self):
        """Keys prefixed with underscore should be preserved."""
        parent = {"mode": "overwrite"}
        child = {"_resolved_threads": [{"id": 1}], "mode": "merge"}
        result = cascade(parent, child)
        assert result["_resolved_threads"] == [{"id": 1}]
        assert result["mode"] == "merge"

    def test_empty_parent(self):
        """Empty parent should return child as-is."""
        parent = {}
        child = {"mode": "merge", "table": "child_table"}
        result = cascade(parent, child)
        assert result == child

    def test_empty_child(self):
        """Empty child should return parent as-is."""
        parent = {"mode": "overwrite", "table": "parent_table"}
        child = {}
        result = cascade(parent, child)
        assert result == parent

    def test_both_empty(self):
        """Both empty should return empty dict."""
        parent = {}
        child = {}
        result = cascade(parent, child)
        assert result == {}

    def test_none_values(self):
        """None values should be handled correctly."""
        parent = {"mode": "overwrite", "nullable": None}
        child = {"mode": None}
        result = cascade(parent, child)
        assert result["mode"] is None  # Child's None wins
        assert result["nullable"] is None  # Parent's None inherited

    def test_nested_structures(self):
        """Nested dicts and lists should replace, not merge."""
        parent = {
            "config": {
                "db": {"host": "localhost", "port": 5432},
                "cache": {"enabled": True},
            }
        }
        child = {
            "config": {
                "db": {"host": "prod.example.com"},  # Missing port
            }
        }
        result = cascade(parent, child)
        # Child's config dict entirely replaces parent's
        assert result["config"]["db"] == {"host": "prod.example.com"}
        assert "cache" not in result["config"]


class TestApplyInheritance:
    """Test apply_inheritance function."""

    def test_thread_only(self):
        """Thread only (no loom or weave defaults) returns thread as-is."""
        thread = {"mode": "merge", "table": "thread_table"}
        result = apply_inheritance(None, None, thread)
        assert result == thread

    def test_weave_to_thread(self):
        """Weave defaults cascade into thread."""
        weave_defaults = {"tags": ["weave"], "mode": "overwrite"}
        thread = {"mode": "merge", "table": "thread_table"}
        result = apply_inheritance(None, weave_defaults, thread)
        assert result["mode"] == "merge"  # Thread wins
        assert result["tags"] == ["weave"]  # From weave
        assert result["table"] == "thread_table"  # From thread

    def test_loom_to_thread(self):
        """Loom defaults cascade into thread (skipping weave)."""
        loom_defaults = {"audit": "enabled", "mode": "overwrite"}
        thread = {"mode": "merge", "table": "thread_table"}
        result = apply_inheritance(loom_defaults, None, thread)
        assert result["mode"] == "merge"  # Thread wins
        assert result["audit"] == "enabled"  # From loom
        assert result["table"] == "thread_table"  # From thread

    def test_full_three_level_cascade(self):
        """Full cascade: loom → weave → thread (thread wins all)."""
        loom_defaults = {"audit": "enabled", "mode": "overwrite", "tags": ["loom"]}
        weave_defaults = {"mode": "append", "tags": ["weave"], "format": "delta"}
        thread = {"mode": "merge", "table": "thread_table"}
        result = apply_inheritance(loom_defaults, weave_defaults, thread)
        # Thread wins mode
        assert result["mode"] == "merge"
        # Weave wins tags (overrides loom)
        assert result["tags"] == ["weave"]
        # Weave wins format (not in loom or thread)
        assert result["format"] == "delta"
        # Loom's audit is inherited (not in weave or thread)
        assert result["audit"] == "enabled"
        # Thread's table is kept
        assert result["table"] == "thread_table"

    def test_priority_order_loom_weave_thread(self):
        """Verify cascade order: thread > weave > loom."""
        loom = {"priority_test": "loom", "loom_only": "loom_value"}
        weave = {"priority_test": "weave", "weave_only": "weave_value"}
        thread = {"priority_test": "thread", "thread_only": "thread_value"}
        result = apply_inheritance(loom, weave, thread)
        # Thread value wins for overlapping key
        assert result["priority_test"] == "thread"
        # Each level's unique keys are preserved
        assert result["loom_only"] == "loom_value"
        assert result["weave_only"] == "weave_value"
        assert result["thread_only"] == "thread_value"

    def test_empty_loom_defaults(self):
        """Empty loom defaults should not affect result."""
        loom = {}
        weave = {"mode": "append"}
        thread = {"table": "thread_table"}
        result = apply_inheritance(loom, weave, thread)
        assert result["mode"] == "append"
        assert result["table"] == "thread_table"

    def test_empty_weave_defaults(self):
        """Empty weave defaults should not affect result."""
        loom = {"audit": "enabled"}
        weave = {}
        thread = {"table": "thread_table"}
        result = apply_inheritance(loom, weave, thread)
        assert result["audit"] == "enabled"
        assert result["table"] == "thread_table"

    def test_empty_thread(self):
        """Empty thread should inherit all from loom and weave."""
        loom = {"audit": "enabled", "mode": "overwrite"}
        weave = {"mode": "append", "format": "delta"}
        thread = {}
        result = apply_inheritance(loom, weave, thread)
        # Weave's mode wins over loom's
        assert result["mode"] == "append"
        # Weave's format is kept
        assert result["format"] == "delta"
        # Loom's audit is inherited
        assert result["audit"] == "enabled"

    def test_dict_replacement_in_cascade(self):
        """Dicts should replace entirely at each cascade level."""
        loom = {"write": {"mode": "overwrite", "format": "delta", "partition": "date"}}
        weave = {"write": {"mode": "merge", "format": "parquet"}}
        thread = {"write": {"mode": "append"}}
        result = apply_inheritance(loom, weave, thread)
        # Thread's write dict entirely replaces earlier levels
        assert result["write"] == {"mode": "append"}
        assert "format" not in result["write"]
        assert "partition" not in result["write"]
