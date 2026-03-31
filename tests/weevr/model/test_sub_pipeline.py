"""Tests for the SubPipeline model."""

import pytest
from pydantic import ValidationError

from weevr.model.sub_pipeline import SubPipeline


class TestSubPipelineFromAlias:
    """Test that the YAML ``from`` key maps to the Python ``from_`` field."""

    def test_from_key_maps_to_from_(self):
        """YAML ``from`` is remapped to Python ``from_``."""
        sp = SubPipeline.model_validate(
            {
                "from": "orders",
                "steps": [{"filter": {"expr": "amount > 0"}}],
            }
        )
        assert sp.from_ == "orders"

    def test_from_underscore_key_accepted(self):
        """Python-native ``from_`` key is accepted directly."""
        sp = SubPipeline.model_validate(
            {
                "from_": "customers",
                "steps": [{"select": {"columns": ["id", "name"]}}],
            }
        )
        assert sp.from_ == "customers"

    def test_missing_from_raises(self):
        """Missing ``from`` / ``from_`` raises ValidationError."""
        with pytest.raises(ValidationError):
            SubPipeline.model_validate(
                {"steps": [{"filter": {"expr": "amount > 0"}}]}
            )


class TestSubPipelineSteps:
    """Test the ``steps`` field validation."""

    def test_valid_steps_accepted(self):
        """A non-empty list of valid steps is accepted."""
        sp = SubPipeline.model_validate(
            {
                "from": "orders",
                "steps": [
                    {"filter": {"expr": "amount > 0"}},
                    {"select": {"columns": ["id", "amount"]}},
                ],
            }
        )
        assert len(sp.steps) == 2

    def test_empty_steps_raises(self):
        """An empty steps list raises ValidationError."""
        with pytest.raises(ValidationError):
            SubPipeline.model_validate({"from": "orders", "steps": []})

    def test_missing_steps_raises(self):
        """Omitting steps entirely raises ValidationError."""
        with pytest.raises(ValidationError):
            SubPipeline.model_validate({"from": "orders"})

    def test_invalid_step_type_raises(self):
        """An unrecognised step type raises ValidationError."""
        with pytest.raises(ValidationError):
            SubPipeline.model_validate(
                {
                    "from": "orders",
                    "steps": [{"not_a_step": {}}],
                }
            )


class TestSubPipelineImmutability:
    """SubPipeline must be immutable (FrozenBase)."""

    def test_frozen(self):
        """Mutation after construction raises ValidationError."""
        sp = SubPipeline.model_validate(
            {
                "from": "orders",
                "steps": [{"filter": {"expr": "amount > 0"}}],
            }
        )
        with pytest.raises(ValidationError):
            sp.from_ = "other"  # type: ignore[misc]
