"""Model validators raise coded errors with ``rule_error``."""

from __future__ import annotations

import pytest
from pydantic import BaseModel, ValidationError, model_validator

from weevr.diagnostics import rule_error
from weevr.diagnostics.pydantic import from_validation_error


class _Rule(BaseModel):
    a: int = 0

    @model_validator(mode="after")
    def _check(self) -> _Rule:
        if self.a < 0:
            raise rule_error(
                "C400", "a must be {positive}", expected=["0", 1], hint="Use a value >= 0."
            )
        return self


def test_code_message_expected_and_hint_reach_the_diagnostic() -> None:
    with pytest.raises(ValidationError) as exc_info:
        _Rule(a=-1)
    [diag] = from_validation_error(exc_info.value, {"a": -1})
    assert diag.code == "C400"
    assert diag.message == "a must be {positive}"
    assert diag.expected == ("0", "1")
    assert diag.hint == "Use a value >= 0."
    assert diag.resolved_path == ""


def test_unregistered_code_is_rejected() -> None:
    with pytest.raises(ValueError, match="Z123"):
        rule_error("Z123", "nope")
