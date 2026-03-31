"""Tests for OneLakeConnection model."""

import pytest
from pydantic import ValidationError

from weevr.model.connection import OneLakeConnection


class TestOneLakeConnection:
    """Test OneLakeConnection model."""

    def test_minimal_connection(self):
        """OneLakeConnection with required fields only."""
        c = OneLakeConnection(
            type="onelake",
            workspace="ws-guid-1234",
            lakehouse="lh-guid-5678",
        )
        assert c.type == "onelake"
        assert c.workspace == "ws-guid-1234"
        assert c.lakehouse == "lh-guid-5678"
        assert c.default_schema is None

    def test_with_default_schema(self):
        """OneLakeConnection with optional default_schema."""
        c = OneLakeConnection(
            type="onelake",
            workspace="ws-guid-1234",
            lakehouse="lh-guid-5678",
            default_schema="silver",
        )
        assert c.default_schema == "silver"

    def test_missing_workspace_raises(self):
        """Missing workspace field raises ValidationError."""
        with pytest.raises(ValidationError):
            OneLakeConnection(type="onelake", lakehouse="lh-guid-5678")  # type: ignore[call-arg]

    def test_missing_lakehouse_raises(self):
        """Missing lakehouse field raises ValidationError."""
        with pytest.raises(ValidationError):
            OneLakeConnection(type="onelake", workspace="ws-guid-1234")  # type: ignore[call-arg]

    def test_invalid_type_raises(self):
        """type value other than 'onelake' raises ValidationError."""
        with pytest.raises(ValidationError):
            OneLakeConnection(
                type="s3",  # type: ignore[arg-type]
                workspace="ws-guid-1234",
                lakehouse="lh-guid-5678",
            )

    def test_missing_type_raises(self):
        """Missing type raises ValidationError (no default provided)."""
        with pytest.raises(ValidationError):
            OneLakeConnection(workspace="ws-guid-1234", lakehouse="lh-guid-5678")  # type: ignore[call-arg]

    def test_frozen(self):
        """OneLakeConnection is immutable."""
        c = OneLakeConnection(
            type="onelake",
            workspace="ws-guid-1234",
            lakehouse="lh-guid-5678",
        )
        with pytest.raises(ValidationError):
            c.workspace = "other"  # type: ignore[misc]

    def test_round_trip(self):
        """OneLakeConnection round-trips through model_dump/model_validate."""
        c = OneLakeConnection(
            type="onelake",
            workspace="ws-guid-1234",
            lakehouse="lh-guid-5678",
            default_schema="gold",
        )
        assert OneLakeConnection.model_validate(c.model_dump()) == c

    def test_all_fields_have_descriptions(self):
        """All fields carry a description in their schema."""
        schema = OneLakeConnection.model_json_schema()
        properties = schema.get("properties", {})
        for field_name in ("type", "workspace", "lakehouse", "default_schema"):
            assert field_name in properties, f"field '{field_name}' missing from schema"
            assert properties[field_name].get("description"), (
                f"field '{field_name}' has no description"
            )
