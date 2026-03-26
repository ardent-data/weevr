"""Tests for runtime resource merge helpers."""

import logging

from weevr.engine.resources import merge_hook_lists, merge_resource_dicts


class TestMergeResourceDicts:
    """Test merge_resource_dicts — child wins per key, parent fills gaps."""

    def test_disjoint_keys_both_present(self):
        """Parent has 'a', child has 'b' → result has both."""
        parent = {"a": {"type": "cache"}}
        child = {"b": {"type": "persist"}}
        result = merge_resource_dicts(parent, child, "resources", "loom", "weave")
        assert result == {"a": {"type": "cache"}, "b": {"type": "persist"}}

    def test_child_key_wins_on_conflict(self):
        """Parent has 'a', child has 'a' → child's 'a' wins."""
        parent = {"a": {"type": "cache", "mode": "MEMORY_ONLY"}}
        child = {"a": {"type": "cache", "mode": "DISK_ONLY"}}
        result = merge_resource_dicts(parent, child, "resources", "loom", "weave")
        assert result == {"a": {"type": "cache", "mode": "DISK_ONLY"}}

    def test_parent_none_child_has_values(self):
        """Parent is None, child has 'b' → result has 'b'."""
        child = {"b": {"type": "persist"}}
        result = merge_resource_dicts(None, child, "resources", "loom", "weave")
        assert result == {"b": {"type": "persist"}}

    def test_child_none_parent_has_values(self):
        """Parent has 'a', child is None → result has 'a'."""
        parent = {"a": {"type": "cache"}}
        result = merge_resource_dicts(parent, None, "resources", "loom", "weave")
        assert result == {"a": {"type": "cache"}}

    def test_both_none_returns_none(self):
        """Both None → returns None."""
        result = merge_resource_dicts(None, None, "resources", "loom", "weave")
        assert result is None

    def test_empty_child_dict_inherits_parent(self):
        """Empty dict at child → inherits parent (empty dict treated as no override)."""
        parent = {"a": {"type": "cache"}}
        result = merge_resource_dicts(parent, {}, "resources", "loom", "weave")
        assert result == {"a": {"type": "cache"}}

    def test_parent_is_copy_not_mutated(self):
        """Merging does not mutate the parent dict."""
        parent = {"a": {"type": "cache"}}
        child = {"b": {"type": "persist"}}
        result = merge_resource_dicts(parent, child, "resources", "loom", "weave")
        assert "b" not in parent
        assert result is not parent


class TestMergeResourceDictsShadowLogging:
    """Test DEBUG logging when child key shadows a parent key."""

    def test_shadow_emits_debug_log(self, caplog):
        """When child key shadows parent key, DEBUG log is emitted."""
        parent = {"shared": {"type": "cache"}}
        child = {"shared": {"type": "persist"}}
        with caplog.at_level(logging.DEBUG, logger="weevr.engine.resources"):
            merge_resource_dicts(parent, child, "resources", "loom", "weave")
        assert any("shared" in record.message for record in caplog.records)
        assert any(record.levelno == logging.DEBUG for record in caplog.records)

    def test_shadow_log_includes_resource_name(self, caplog):
        """Shadow log message includes the resource name."""
        parent = {"my_cache": {"type": "cache"}}
        child = {"my_cache": {"type": "persist"}}
        with caplog.at_level(logging.DEBUG, logger="weevr.engine.resources"):
            merge_resource_dicts(parent, child, "resources", "loom", "weave")
        assert any("my_cache" in record.message for record in caplog.records)

    def test_shadow_log_includes_parent_level(self, caplog):
        """Shadow log message includes the parent level."""
        parent = {"r": {"type": "cache"}}
        child = {"r": {"type": "persist"}}
        with caplog.at_level(logging.DEBUG, logger="weevr.engine.resources"):
            merge_resource_dicts(parent, child, "resources", "loom", "weave")
        assert any("loom" in record.message for record in caplog.records)

    def test_shadow_log_includes_child_level(self, caplog):
        """Shadow log message includes the child level."""
        parent = {"r": {"type": "cache"}}
        child = {"r": {"type": "persist"}}
        with caplog.at_level(logging.DEBUG, logger="weevr.engine.resources"):
            merge_resource_dicts(parent, child, "resources", "loom", "weave")
        assert any("weave" in record.message for record in caplog.records)

    def test_no_shadow_log_for_new_keys(self, caplog):
        """No shadow log when child introduces keys not in parent."""
        parent = {"a": {"type": "cache"}}
        child = {"b": {"type": "persist"}}
        with caplog.at_level(logging.DEBUG, logger="weevr.engine.resources"):
            merge_resource_dicts(parent, child, "resources", "loom", "weave")
        assert not any(
            record.levelno == logging.DEBUG and "shadows" in record.message
            for record in caplog.records
        )

    def test_multiple_shadows_each_logged(self, caplog):
        """Each shadowed key gets its own log entry."""
        parent = {"x": {}, "y": {}}
        child = {"x": {"new": True}, "y": {"new": True}}
        with caplog.at_level(logging.DEBUG, logger="weevr.engine.resources"):
            merge_resource_dicts(parent, child, "resources", "loom", "weave")
        shadow_logs = [r for r in caplog.records if "shadows" in r.message]
        assert len(shadow_logs) == 2


class TestMergeHookLists:
    """Test merge_hook_lists — child list replaces parent entirely."""

    def test_both_present_child_wins(self):
        """Parent has items, child has items → child list wins entirely."""
        parent = ["step_a", "step_b"]
        child = ["step_c"]
        result = merge_hook_lists(parent, child)
        assert result == ["step_c"]

    def test_child_none_parent_inherited(self):
        """Parent has items, child is None → parent inherited."""
        parent = ["step_a"]
        result = merge_hook_lists(parent, None)
        assert result == ["step_a"]

    def test_parent_none_child_used(self):
        """Parent is None, child has items → child used."""
        child = ["step_c"]
        result = merge_hook_lists(None, child)
        assert result == ["step_c"]

    def test_both_none_returns_none(self):
        """Both None → returns None."""
        result = merge_hook_lists(None, None)
        assert result is None

    def test_child_list_not_mutated(self):
        """Returned list is not the same object as child (safe copy)."""
        child = ["step_a"]
        result = merge_hook_lists(["step_x"], child)
        assert result == child
        assert result is not child
