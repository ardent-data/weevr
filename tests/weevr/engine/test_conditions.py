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

    def test_not_and_precedence(self):
        """'not' binds tighter than 'and': not True and False → False."""
        cond = ConditionSpec(when="not True and False")
        assert evaluate_condition(cond) is False

    def test_not_or_precedence(self):
        """'not' binds tighter than 'or': not False or False → True."""
        cond = ConditionSpec(when="not False or False")
        assert evaluate_condition(cond) is True

    def test_or_and_precedence(self):
        """'and' binds tighter than 'or': True or False and False → True."""
        cond = ConditionSpec(when="True or False and False")
        assert evaluate_condition(cond) is True

    def test_invalid_expression_raises(self):
        """Malformed expression raises ConfigError."""
        cond = ConditionSpec(when="??? invalid")
        with pytest.raises(ConfigError, match="Cannot evaluate"):
            evaluate_condition(cond)


@pytest.mark.spark
class TestBuiltinMemo:
    """Memoization of builtin probes within one evaluation batch."""

    def _counting_read(self, monkeypatch):
        import weevr.engine.conditions as conditions_mod

        calls = {"n": 0}
        real_read = conditions_mod.read_delta

        def counting(spark_arg, path_arg):
            calls["n"] += 1
            return real_read(spark_arg, path_arg)

        monkeypatch.setattr(conditions_mod, "read_delta", counting)
        return calls

    def test_shared_memo_computes_builtin_once(self, spark, tmp_delta_path, monkeypatch):
        from spark_helpers import create_delta_table

        path = tmp_delta_path("memo_shared")
        create_delta_table(spark, path, [{"id": 1}, {"id": 2}])
        calls = self._counting_read(monkeypatch)

        memo: dict = {}
        for _ in range(3):
            cond = ConditionSpec(when=f"row_count('{path}') > 0")
            assert evaluate_condition(cond, spark=spark, memo=memo) is True
        assert calls["n"] == 1

    def test_distinct_arguments_memoized_separately(self, spark, tmp_delta_path, monkeypatch):
        from spark_helpers import create_delta_table

        path_a = tmp_delta_path("memo_arg_a")
        path_b = tmp_delta_path("memo_arg_b")
        create_delta_table(spark, path_a, [{"id": 1}])
        create_delta_table(spark, path_b, [{"id": 1}, {"id": 2}])
        calls = self._counting_read(monkeypatch)

        memo: dict = {}
        assert (
            evaluate_condition(
                ConditionSpec(when=f"row_count('{path_a}') > 1"), spark=spark, memo=memo
            )
            is False
        )
        assert (
            evaluate_condition(
                ConditionSpec(when=f"row_count('{path_b}') > 1"), spark=spark, memo=memo
            )
            is True
        )
        assert calls["n"] == 2

    def test_fresh_containers_observe_new_writes(self, spark, tmp_delta_path, monkeypatch):
        """A new memo container sees writes made after the previous batch."""
        from spark_helpers import create_delta_table

        path = tmp_delta_path("memo_fresh")
        create_delta_table(spark, path, [{"id": 1}])
        cond = ConditionSpec(when=f"row_count('{path}') > 1")

        batch_one: dict = {}
        assert evaluate_condition(cond, spark=spark, memo=batch_one) is False

        spark.createDataFrame([(2,)], "id BIGINT").write.format("delta").mode("append").save(path)

        batch_two: dict = {}
        assert evaluate_condition(cond, spark=spark, memo=batch_two) is True

    def test_no_memo_evaluates_fresh_every_time(self, spark, tmp_delta_path, monkeypatch):
        from spark_helpers import create_delta_table

        path = tmp_delta_path("memo_absent")
        create_delta_table(spark, path, [{"id": 1}])
        calls = self._counting_read(monkeypatch)

        cond = ConditionSpec(when=f"row_count('{path}') > 0")
        assert evaluate_condition(cond, spark=spark) is True
        assert evaluate_condition(cond, spark=spark) is True
        assert calls["n"] == 2
