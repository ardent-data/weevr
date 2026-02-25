"""Condition evaluation engine for conditional execution."""

from __future__ import annotations

import logging
import re
from typing import Any

from pyspark.sql import SparkSession

from weevr.errors.exceptions import ConfigError, ExecutionError
from weevr.model.weave import ConditionSpec

logger = logging.getLogger(__name__)

# Pattern for ${param.name} references
_PARAM_REF = re.compile(r"\$\{param\.([^}]+)\}")

# Pattern for built-in function calls: func_name('arg')
_BUILTIN_CALL = re.compile(r"(table_exists|table_empty|row_count)\s*\(\s*'([^']+)'\s*\)")


def _table_exists(spark: SparkSession, path: str) -> bool:
    """Check if a Delta table exists at the given path."""
    try:
        from delta.tables import DeltaTable

        return DeltaTable.isDeltaTable(spark, path)
    except Exception:
        return False


def _table_empty(spark: SparkSession, path: str) -> bool:
    """Check if a table is empty or doesn't exist."""
    if not _table_exists(spark, path):
        return True
    try:
        return spark.read.format("delta").load(path).limit(1).count() == 0
    except Exception:
        return True


def _row_count(spark: SparkSession, path: str) -> int:
    """Count rows in a Delta table. Returns 0 if table doesn't exist."""
    if not _table_exists(spark, path):
        return 0
    try:
        return spark.read.format("delta").load(path).count()
    except Exception:
        return 0


_BUILTIN_FUNCS: dict[str, Any] = {
    "table_exists": _table_exists,
    "table_empty": _table_empty,
    "row_count": _row_count,
}


def _resolve_params(expr: str, params: dict[str, Any]) -> str:
    """Replace ${param.name} references with parameter values."""

    def _replace(match: re.Match[str]) -> str:
        param_name = match.group(1)
        if param_name not in params:
            raise ConfigError(
                f"Unresolved parameter reference '${{param.{param_name}}}' in condition",
                config_key="condition",
            )
        return str(params[param_name])

    return _PARAM_REF.sub(_replace, expr)


def _evaluate_builtins(expr: str, spark: SparkSession | None) -> str:
    """Replace built-in function calls with their boolean/int results."""

    def _replace(match: re.Match[str]) -> str:
        func_name = match.group(1)
        arg = match.group(2)
        if spark is None:
            raise ExecutionError(f"Built-in check '{func_name}' requires a SparkSession")
        result = _BUILTIN_FUNCS[func_name](spark, arg)
        if isinstance(result, bool):
            return "True" if result else "False"
        return str(result)

    return _BUILTIN_CALL.sub(_replace, expr)


def _safe_eval(expr: str) -> bool:
    """Evaluate a simple boolean expression safely (no arbitrary code execution).

    Supports: ==, !=, <, >, <=, >=, not, and, or, True, False,
    string literals, numeric literals.
    """
    expr = expr.strip()

    # Split by lowest-precedence operator first: or
    or_parts = _split_operator(expr, " or ")
    if len(or_parts) > 1:
        return any(_safe_eval(p) for p in or_parts)

    # Then: and
    and_parts = _split_operator(expr, " and ")
    if len(and_parts) > 1:
        return all(_safe_eval(p) for p in and_parts)

    # Highest unary precedence: not
    if expr.lower().startswith("not "):
        return not _safe_eval(expr[4:])

    # Handle comparison operators (keep values as comparable types, not booleans)
    for op in ("!=", "==", "<=", ">=", "<", ">"):
        parts = expr.split(op, 1)
        if len(parts) == 2:
            left = _parse_comparable(parts[0].strip())
            right = _parse_comparable(parts[1].strip())
            if op == "==":
                return left == right
            if op == "!=":
                return left != right
            if op == "<":
                return left < right  # type: ignore[operator]
            if op == ">":
                return left > right  # type: ignore[operator]
            if op == "<=":
                return left <= right  # type: ignore[operator]
            if op == ">=":
                return left >= right  # type: ignore[operator]

    # Direct boolean
    val = _parse_value(expr)
    if isinstance(val, bool):
        return val

    raise ConfigError(f"Cannot evaluate condition expression: '{expr}'", config_key="condition")


def _split_operator(expr: str, operator: str) -> list[str]:
    """Split expression by operator, respecting quoted strings."""
    parts = []
    depth = 0
    current: list[str] = []
    i = 0
    while i < len(expr):
        if expr[i] == "'":
            depth = 1 - depth
            current.append(expr[i])
        elif depth == 0 and expr[i:].startswith(operator):
            parts.append("".join(current))
            current = []
            i += len(operator)
            continue
        else:
            current.append(expr[i])
        i += 1
    parts.append("".join(current))
    return parts


def _parse_comparable(s: str) -> Any:
    """Parse a string for comparison — strips quotes and parses numbers but not booleans."""
    s = s.strip()
    if (s.startswith("'") and s.endswith("'")) or (s.startswith('"') and s.endswith('"')):
        return s[1:-1]
    try:
        return int(s)
    except ValueError:
        pass
    try:
        return float(s)
    except ValueError:
        pass
    return s


def _parse_value(s: str) -> Any:
    """Parse a string into a typed value for comparison."""
    s = s.strip()
    if s.lower() == "true":
        return True
    if s.lower() == "false":
        return False
    if (s.startswith("'") and s.endswith("'")) or (s.startswith('"') and s.endswith('"')):
        return s[1:-1]
    try:
        return int(s)
    except ValueError:
        pass
    try:
        return float(s)
    except ValueError:
        pass
    return s


def evaluate_condition(
    condition: ConditionSpec,
    spark: SparkSession | None = None,
    params: dict[str, Any] | None = None,
) -> bool:
    """Evaluate a condition specification.

    Resolves parameter references, evaluates built-in function calls,
    and evaluates the resulting boolean expression.

    Args:
        condition: The condition to evaluate.
        spark: SparkSession for built-in table checks. Required only if the
            condition uses ``table_exists()``, ``table_empty()``, or
            ``row_count()``.
        params: Parameter dictionary for ``${param.x}`` references.

    Returns:
        True if the condition is met, False otherwise.

    Raises:
        ConfigError: If the condition expression is malformed or references
            an unresolved parameter.
        ExecutionError: If a built-in check fails (e.g., Spark catalog error).
    """
    expr = condition.when
    resolved_params = params or {}

    # Step 1: Resolve parameter references
    expr = _resolve_params(expr, resolved_params)

    # Step 2: Evaluate built-in function calls
    expr = _evaluate_builtins(expr, spark)

    # Step 3: Evaluate boolean expression
    return _safe_eval(expr)
