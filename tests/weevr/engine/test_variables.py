"""Tests for VariableContext runtime context."""

import pytest

from weevr.engine.variables import VariableContext
from weevr.model.variable import VariableSpec


def _specs(**kwargs: str) -> dict[str, VariableSpec]:
    """Build a specs dict from type shorthand."""
    return {name: VariableSpec(type=t) for name, t in kwargs.items()}  # type: ignore[arg-type]


class TestVariableContextInit:
    """Test VariableContext initialization."""

    def test_empty_specs(self):
        """Empty specs creates empty context."""
        ctx = VariableContext()
        assert ctx.snapshot() == {}

    def test_none_specs(self):
        """None specs treated as empty."""
        ctx = VariableContext(None)
        assert ctx.snapshot() == {}

    def test_defaults_populated(self):
        """Default values from specs are populated on init."""
        specs = {
            "batch_id": VariableSpec(type="string", default="abc"),
            "count": VariableSpec(type="int", default=0),
        }
        ctx = VariableContext(specs)
        assert ctx.get("batch_id") == "abc"
        assert ctx.get("count") == 0

    def test_no_default_returns_none(self):
        """Variable without default returns None."""
        ctx = VariableContext(_specs(x="string"))
        assert ctx.get("x") is None


class TestVariableContextGetSet:
    """Test get/set operations."""

    def test_set_and_get(self):
        """Set then get returns the value."""
        ctx = VariableContext(_specs(name="string"))
        ctx.set("name", "hello")
        assert ctx.get("name") == "hello"

    def test_set_overrides_default(self):
        """Set overrides the default value."""
        specs = {"x": VariableSpec(type="int", default=10)}
        ctx = VariableContext(specs)
        ctx.set("x", 42)
        assert ctx.get("x") == 42

    def test_set_none_clears_value(self):
        """Setting None clears the variable (null handling)."""
        specs = {"x": VariableSpec(type="int", default=5)}
        ctx = VariableContext(specs)
        ctx.set("x", None)
        assert ctx.get("x") is None

    def test_get_undeclared_raises(self):
        """Getting an undeclared variable raises KeyError."""
        ctx = VariableContext(_specs(x="int"))
        with pytest.raises(KeyError, match="not declared"):
            ctx.get("unknown")

    def test_set_undeclared_raises(self):
        """Setting an undeclared variable raises KeyError."""
        ctx = VariableContext(_specs(x="int"))
        with pytest.raises(KeyError, match="not declared"):
            ctx.set("unknown", 42)


class TestVariableContextTypeCasting:
    """Test type validation and casting."""

    def test_int_from_string(self):
        """String value cast to int."""
        ctx = VariableContext(_specs(count="int"))
        ctx.set("count", "42")
        assert ctx.get("count") == 42

    def test_float_from_string(self):
        """String value cast to float."""
        ctx = VariableContext(_specs(rate="double"))
        ctx.set("rate", "3.14")
        assert ctx.get("rate") == pytest.approx(3.14)

    def test_bool_native(self):
        """Native bool value accepted."""
        ctx = VariableContext(_specs(flag="boolean"))
        ctx.set("flag", True)
        assert ctx.get("flag") is True

    def test_string_passthrough(self):
        """String value stored as-is for string type."""
        ctx = VariableContext(_specs(msg="string"))
        ctx.set("msg", "hello")
        assert ctx.get("msg") == "hello"

    def test_invalid_cast_raises(self):
        """Value that can't be cast raises TypeError."""
        ctx = VariableContext(_specs(count="int"))
        with pytest.raises(TypeError, match="Cannot cast"):
            ctx.set("count", "not_a_number")


class TestVariableContextResolveRefs:
    """Test ${var.name} string interpolation."""

    def test_single_ref(self):
        """Single variable reference resolved."""
        ctx = VariableContext(_specs(env="string"))
        ctx.set("env", "prod")
        assert ctx.resolve_refs("Running in ${var.env}") == "Running in prod"

    def test_multiple_refs(self):
        """Multiple variable references resolved."""
        ctx = VariableContext(_specs(a="string", b="int"))
        ctx.set("a", "hello")
        ctx.set("b", 42)
        assert ctx.resolve_refs("${var.a} ${var.b}") == "hello 42"

    def test_undeclared_ref_left_as_is(self):
        """Undeclared variable reference left unchanged."""
        ctx = VariableContext(_specs(x="string"))
        assert ctx.resolve_refs("${var.unknown}") == "${var.unknown}"

    def test_unset_ref_left_as_is(self):
        """Declared but unset variable reference left unchanged."""
        ctx = VariableContext(_specs(x="string"))
        assert ctx.resolve_refs("${var.x}") == "${var.x}"

    def test_no_refs_passthrough(self):
        """String without variable refs returned unchanged."""
        ctx = VariableContext()
        assert ctx.resolve_refs("no refs here") == "no refs here"

    def test_param_refs_not_touched(self):
        """${param.name} refs are not resolved by VariableContext."""
        ctx = VariableContext(_specs(x="string"))
        ctx.set("x", "val")
        text = "${param.env} and ${var.x}"
        assert ctx.resolve_refs(text) == "${param.env} and val"


class TestVariableContextSnapshot:
    """Test snapshot for telemetry."""

    def test_snapshot_returns_copy(self):
        """Snapshot returns a copy, not a reference."""
        ctx = VariableContext(_specs(x="int"))
        ctx.set("x", 1)
        snap = ctx.snapshot()
        ctx.set("x", 2)
        assert snap["x"] == 1
        assert ctx.get("x") == 2

    def test_snapshot_includes_defaults(self):
        """Snapshot includes values from defaults."""
        specs = {"x": VariableSpec(type="int", default=10)}
        ctx = VariableContext(specs)
        assert ctx.snapshot() == {"x": 10}

    def test_snapshot_empty_context(self):
        """Empty context produces empty snapshot."""
        ctx = VariableContext()
        assert ctx.snapshot() == {}
