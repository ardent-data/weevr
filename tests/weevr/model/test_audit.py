"""Tests for AuditTemplate model."""

import pytest
from pydantic import ValidationError

from weevr.model.audit import AuditTemplate
from weevr.model.loom import Loom
from weevr.model.thread import Thread
from weevr.model.weave import Weave


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


class TestAuditTemplateFlatShorthand:
    """A flat column map is accepted as shorthand for the nested columns form."""

    def test_flat_equals_nested(self):
        """A flat map validates to the same template as its nested form."""
        cols = {"_load_ts": "current_timestamp()", "_run_id": "'r1'"}
        assert AuditTemplate.model_validate(cols) == AuditTemplate(columns=cols)

    def test_flat_empty(self):
        """An empty flat map yields a template with no columns."""
        assert AuditTemplate.model_validate({}) == AuditTemplate(columns={})

    def test_flat_non_string_value_rejected(self):
        """A flat map whose value is not a string expression is rejected."""
        with pytest.raises(ValidationError):
            AuditTemplate.model_validate({"_load_ts": ["current_timestamp()"]})

    def test_flat_column_named_columns_rejected(self):
        """A dict with a columns key is the nested form, so a string value is rejected."""
        with pytest.raises(ValidationError):
            AuditTemplate.model_validate({"columns": "current_timestamp()"})

    def test_nested_extra_key_still_ignored(self):
        """The nested form keeps ignoring unknown keys alongside columns."""
        at = AuditTemplate.model_validate({"columns": {"ts": "now()"}, "note": "x"})
        assert at == AuditTemplate(columns={"ts": "now()"})


class TestAuditTemplateJsonSchema:
    """The published JSON schema accepts both audit template forms."""

    def _schema(self, model: type) -> dict:
        return model.model_json_schema()["$defs"]["AuditTemplate"]

    @pytest.mark.parametrize("model", [Loom, Weave, Thread])
    def test_any_of_nested_and_flat(self, model):
        """Each top-level schema offers the nested and the flat form."""
        nested, flat = self._schema(model)["anyOf"]
        assert nested["required"] == ["columns"]
        assert nested["properties"]["columns"]["additionalProperties"] == {"type": "string"}
        assert flat == {"type": "object", "additionalProperties": {"type": "string"}}

    def test_title_and_description_kept(self):
        """The definition keeps its title and docstring description."""
        schema = self._schema(Loom)
        assert schema["title"] == "AuditTemplate"
        assert schema["description"].startswith("A named set of audit columns")

    def test_serialization_schema_is_nested_only(self):
        """Dumps are always nested, so the serialization schema omits the flat form."""
        schema = Loom.model_json_schema(mode="serialization")["$defs"]["AuditTemplate"]
        assert "anyOf" not in schema
        assert schema["required"] == ["columns"]
