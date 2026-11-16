"""Tests for hook list resolution."""

from weevr.engine.resources import merge_hook_lists


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
