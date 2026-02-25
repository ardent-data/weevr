"""Tests for condition evaluation engine."""

import pytest

from weevr.engine.conditions import evaluate_condition
from weevr.errors.exceptions import ConfigError
from weevr.model.weave import ConditionSpec


class TestParamConditions:
    """Test parameter-based condition evaluation (no Spark needed)."""

    def test_param_equality_true(self):
        """Param equality resolves and evaluates to True."""
        cond = ConditionSpec(when="${param.mode} == 'full'")
        assert evaluate_condition(cond, params={"mode": "full"}) is True

    def test_param_equality_false(self):
        """Param equality resolves and evaluates to False."""
        cond = ConditionSpec(when="${param.mode} == 'full'")
        assert evaluate_condition(cond, params={"mode": "incremental"}) is False

    def test_param_inequality_true(self):
        """Param != operator evaluates correctly."""
        cond = ConditionSpec(when="${param.mode} != 'full'")
        assert evaluate_condition(cond, params={"mode": "incremental"}) is True

    def test_param_inequality_false(self):
        """Param != when values match evaluates to False."""
        cond = ConditionSpec(when="${param.mode} != 'full'")
        assert evaluate_condition(cond, params={"mode": "full"}) is False

    def test_unresolved_param_raises(self):
        """Missing parameter raises ConfigError."""
        cond = ConditionSpec(when="${param.missing} == 'value'")
        with pytest.raises(ConfigError, match="Unresolved parameter"):
            evaluate_condition(cond, params={})

    def test_unresolved_param_no_params_raises(self):
        """None params with param reference raises ConfigError."""
        cond = ConditionSpec(when="${param.x} == 'y'")
        with pytest.raises(ConfigError, match="Unresolved parameter"):
            evaluate_condition(cond)

    def test_numeric_comparison(self):
        """Numeric parameter comparison works."""
        cond = ConditionSpec(when="${param.threshold} > 100")
        assert evaluate_condition(cond, params={"threshold": "200"}) is True
        assert evaluate_condition(cond, params={"threshold": "50"}) is False

    def test_boolean_true_literal(self):
        """Literal 'true' evaluates to True."""
        cond = ConditionSpec(when="true")
        assert evaluate_condition(cond) is True

    def test_boolean_false_literal(self):
        """Literal 'false' evaluates to False."""
        cond = ConditionSpec(when="false")
        assert evaluate_condition(cond) is False

    def test_not_operator(self):
        """'not' prefix inverts the result."""
        cond = ConditionSpec(when="not false")
        assert evaluate_condition(cond) is True

        cond2 = ConditionSpec(when="not true")
        assert evaluate_condition(cond2) is False

    def test_and_operator(self):
        """'and' operator combines conditions."""
        cond = ConditionSpec(when="true and true")
        assert evaluate_condition(cond) is True

        cond2 = ConditionSpec(when="true and false")
        assert evaluate_condition(cond2) is False

    def test_or_operator(self):
        """'or' operator combines conditions."""
        cond = ConditionSpec(when="false or true")
        assert evaluate_condition(cond) is True

        cond2 = ConditionSpec(when="false or false")
        assert evaluate_condition(cond2) is False

    def test_combined_param_and_literal(self):
        """Param condition combined with boolean operator."""
        cond = ConditionSpec(when="${param.enabled} == 'true' and ${param.env} == 'prod'")
        assert evaluate_condition(cond, params={"enabled": "true", "env": "prod"}) is True
        assert evaluate_condition(cond, params={"enabled": "true", "env": "dev"}) is False

    def test_invalid_expression_raises(self):
        """Malformed expression raises ConfigError."""
        cond = ConditionSpec(when="??? invalid")
        with pytest.raises(ConfigError, match="Cannot evaluate"):
            evaluate_condition(cond)
