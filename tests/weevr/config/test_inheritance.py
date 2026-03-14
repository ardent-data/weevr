"""Tests for config inheritance cascade."""

from typing import Any

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
        parent = {"template": "standard", "tags": ["parent"]}
        child = {"mode": "merge"}
        result = cascade(parent, child)
        assert result["template"] == "standard"  # Inherited
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

    def test_execution_config_cascade(self):
        """Execution config cascades from loom through weave to thread."""
        loom = {"execution": {"log_level": "minimal", "trace": True}}
        weave = {}
        thread = {"sources": {"s": "t"}, "target": {"table": "out"}}
        result = apply_inheritance(loom, weave, thread)
        assert result["execution"] == {"log_level": "minimal", "trace": True}

    def test_execution_config_thread_overrides(self):
        """Thread execution config replaces loom execution entirely."""
        loom = {"execution": {"log_level": "minimal", "trace": True}}
        weave = {}
        thread = {"execution": {"log_level": "debug"}}
        result = apply_inheritance(loom, weave, thread)
        # Thread's execution dict replaces entirely (no deep merge)
        assert result["execution"] == {"log_level": "debug"}
        assert "trace" not in result["execution"]

    def test_execution_config_weave_overrides_loom(self):
        """Weave execution config replaces loom execution."""
        loom = {"execution": {"log_level": "minimal", "trace": True}}
        weave = {"execution": {"log_level": "verbose", "trace": False}}
        thread = {}
        result = apply_inheritance(loom, weave, thread)
        assert result["execution"] == {"log_level": "verbose", "trace": False}

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


class TestNamingInheritance:
    """Test naming config cascade through loom → weave → thread levels."""

    def test_loom_naming_cascades_to_thread(self):
        """Loom naming inherited by thread when thread has no naming."""
        loom = {"naming": {"columns": "snake_case"}}
        thread = {"target": {"alias": "data.output"}}
        result = apply_inheritance(loom, None, thread)
        assert result["target"]["naming"] == {"columns": "snake_case"}

    def test_weave_naming_overrides_loom(self):
        """Weave naming overrides loom naming."""
        loom = {"naming": {"columns": "snake_case"}}
        weave = {"naming": {"columns": "camelCase"}}
        thread = {"target": {"alias": "data.output"}}
        result = apply_inheritance(loom, weave, thread)
        assert result["target"]["naming"] == {"columns": "camelCase"}

    def test_thread_naming_overrides_weave(self):
        """Thread target naming overrides weave naming."""
        weave = {"naming": {"columns": "camelCase"}}
        thread = {"target": {"alias": "data.output", "naming": {"columns": "PascalCase"}}}
        result = apply_inheritance(None, weave, thread)
        # Thread already has naming, so parent naming is NOT applied
        assert result["target"]["naming"] == {"columns": "PascalCase"}

    def test_thread_opt_out_with_none(self):
        """Thread can opt out of naming with columns=none."""
        loom = {"naming": {"columns": "snake_case"}}
        thread = {"target": {"alias": "data.output", "naming": {"columns": "none"}}}
        result = apply_inheritance(loom, None, thread)
        assert result["target"]["naming"] == {"columns": "none"}

    def test_no_naming_at_any_level(self):
        """No naming config anywhere → no naming key in target."""
        thread = {"target": {"alias": "data.output"}}
        result = apply_inheritance(None, None, thread)
        assert "naming" not in result.get("target", {})

    def test_exclude_list_inherited(self):
        """Loom exclude list inherited when thread has no naming."""
        loom = {"naming": {"columns": "snake_case", "exclude": ["__*"]}}
        thread = {"target": {"alias": "data.output"}}
        result = apply_inheritance(loom, None, thread)
        assert result["target"]["naming"]["exclude"] == ["__*"]

    def test_thread_naming_replaces_parent_entirely(self):
        """Thread naming replaces parent entirely (not merged field-by-field)."""
        loom = {"naming": {"columns": "snake_case", "exclude": ["__*"]}}
        thread = {"target": {"alias": "data.output", "naming": {"columns": "camelCase"}}}
        result = apply_inheritance(loom, None, thread)
        # Thread had naming, so loom naming is NOT applied
        assert result["target"]["naming"] == {"columns": "camelCase"}
        assert "exclude" not in result["target"]["naming"]


def _hook_step(check: str, name: str | None = None) -> dict[str, Any]:
    """Build a minimal quality_gate hook step dict for testing."""
    step: dict[str, Any] = {"type": "quality_gate", "check": check}
    if name:
        step["name"] = name
    return step


class TestHookInheritance:
    """Verify hooks, lookups, and variables cascade correctly (DEC-010)."""

    def test_weave_pre_steps_replace_loom_defaults(self):
        """Weave pre_steps entirely replaces loom defaults pre_steps."""
        loom: dict[str, Any] = {
            "pre_steps": [_hook_step("table_exists", "loom_gate")],
        }
        weave: dict[str, Any] = {
            "pre_steps": [_hook_step("row_count", "weave_gate")],
        }
        result = cascade(loom, weave)
        assert len(result["pre_steps"]) == 1
        assert result["pre_steps"][0]["check"] == "row_count"

    def test_weave_post_steps_replace_loom_defaults(self):
        """Weave post_steps entirely replaces loom defaults post_steps."""
        loom: dict[str, Any] = {
            "post_steps": [
                {"type": "log_message", "message": "loom done"},
                {"type": "log_message", "message": "loom extra"},
            ],
        }
        weave: dict[str, Any] = {
            "post_steps": [{"type": "log_message", "message": "weave done"}],
        }
        result = cascade(loom, weave)
        assert len(result["post_steps"]) == 1
        assert result["post_steps"][0]["message"] == "weave done"

    def test_lookups_replaced_entirely(self):
        """Weave lookups dict replaces loom defaults lookups entirely."""
        loom: dict[str, Any] = {
            "lookups": {
                "ref_a": {"source": {"type": "delta", "alias": "db.ref_a"}, "materialize": True},
                "ref_b": {"source": {"type": "delta", "alias": "db.ref_b"}},
            },
        }
        weave: dict[str, Any] = {
            "lookups": {
                "ref_c": {"source": {"type": "delta", "alias": "db.ref_c"}},
            },
        }
        result = cascade(loom, weave)
        # Weave's lookups dict replaces entirely — only ref_c remains
        assert "ref_c" in result["lookups"]
        assert "ref_a" not in result["lookups"]
        assert "ref_b" not in result["lookups"]

    def test_variables_replaced_entirely(self):
        """Weave variables dict replaces loom defaults variables entirely."""
        loom: dict[str, Any] = {
            "variables": {
                "batch_id": {"type": "string", "default": "loom-default"},
                "run_ts": {"type": "timestamp"},
            },
        }
        weave: dict[str, Any] = {
            "variables": {
                "batch_id": {"type": "string", "default": "weave-override"},
            },
        }
        result = cascade(loom, weave)
        # Weave replaces entirely — only batch_id with weave value, no run_ts
        assert result["variables"]["batch_id"]["default"] == "weave-override"
        assert "run_ts" not in result["variables"]

    def test_loom_defaults_inherited_when_weave_absent(self):
        """Loom hooks/lookups/variables inherited when weave has none."""
        loom: dict[str, Any] = {
            "pre_steps": [_hook_step("source_freshness", "freshness_check")],
            "lookups": {"ref": {"source": {"type": "delta", "alias": "db.ref"}}},
            "variables": {"run_ts": {"type": "timestamp"}},
        }
        weave: dict[str, Any] = {"name": "my_weave"}
        result = cascade(loom, weave)
        # All loom defaults inherited
        assert len(result["pre_steps"]) == 1
        assert result["pre_steps"][0]["check"] == "source_freshness"
        assert "ref" in result["lookups"]
        assert "run_ts" in result["variables"]
        assert result["name"] == "my_weave"

    def test_hooks_in_three_level_cascade(self):
        """Full loom → weave → thread cascade with hook keys at loom level."""
        loom: dict[str, Any] = {
            "pre_steps": [_hook_step("table_exists")],
            "variables": {"v1": {"type": "string"}},
            "mode": "overwrite",
        }
        weave: dict[str, Any] = {"mode": "append"}
        thread: dict[str, Any] = {"mode": "merge", "table": "out"}
        result = apply_inheritance(loom, weave, thread)
        # Thread wins mode
        assert result["mode"] == "merge"
        # Loom's pre_steps inherited (weave didn't override)
        assert result["pre_steps"][0]["check"] == "table_exists"
        # Loom's variables inherited
        assert "v1" in result["variables"]
        # Thread's table kept
        assert result["table"] == "out"

    def test_weave_hooks_override_loom_in_cascade(self):
        """Weave hooks replace loom hooks when both present in cascade."""
        loom: dict[str, Any] = {
            "pre_steps": [_hook_step("table_exists", "loom_check")],
        }
        weave: dict[str, Any] = {
            "pre_steps": [_hook_step("row_count", "weave_check")],
        }
        thread: dict[str, Any] = {"table": "out"}
        result = apply_inheritance(loom, weave, thread)
        assert len(result["pre_steps"]) == 1
        assert result["pre_steps"][0]["name"] == "weave_check"
