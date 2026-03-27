"""Tests for AuditTemplate model."""

import pytest
from pydantic import ValidationError

from weevr.model.audit import AuditTemplate


class TestAuditTemplate:
    """Test AuditTemplate model."""

    def test_valid_construction(self):
        """AuditTemplate with populated columns dict is valid."""
        cols = {"created_at": "current_timestamp()", "created_by": "current_user()"}
        at = AuditTemplate(columns=cols)
        assert at.columns == cols

    def test_empty_columns(self):
        """AuditTemplate with empty columns dict is valid."""
        at = AuditTemplate(columns={})
        assert at.columns == {}

    def test_frozen(self):
        """AuditTemplate is immutable."""
        at = AuditTemplate(columns={"ts": "current_timestamp()"})
        with pytest.raises(ValidationError):
            at.columns = {}  # type: ignore[misc]

    def test_round_trip(self):
        """AuditTemplate round-trips via model_dump and model_validate."""
        at = AuditTemplate(
            columns={
                "updated_at": "current_timestamp()",
                "row_hash": "sha2(concat_ws('|', *), 256)",
            }
        )
        assert AuditTemplate.model_validate(at.model_dump()) == at
