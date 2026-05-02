"""Pipeline conditional step handlers — case_when."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from weevr.model.pipeline import CaseWhenParams


def apply_case_when(df: DataFrame, params: CaseWhenParams) -> DataFrame:
    """Apply conditional column assignment using when/then/otherwise logic.

    Args:
        df: Input DataFrame.
        params: Case-when parameters — output column, cases list, optional otherwise.

    Returns:
        DataFrame with the output column set to the first matching case value,
        or the otherwise value, or null if no match and no otherwise.
    """
    expr = F.when(F.expr(params.cases[0].when), F.expr(params.cases[0].then))
    for branch in params.cases[1:]:
        expr = expr.when(F.expr(branch.when), F.expr(branch.then))
    if params.otherwise is not None:
        expr = expr.otherwise(F.expr(params.otherwise))
    return df.withColumn(params.column, expr)
