"""Hashing operations — surrogate key generation and change detection."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from weevr.errors.exceptions import ExecutionError
from weevr.model.keys import ChangeDetectionConfig, KeyConfig, SurrogateKeyConfig

# Sentinel value substituted for NULL before hashing, ensuring consistent output.
_NULL_SENTINEL = "__NULL__"

# Separator used when concatenating multiple key columns into a single string.
_KEY_SEPARATOR = "|"


def compute_keys(df: DataFrame, keys: KeyConfig) -> DataFrame:
    """Apply key and hash computations declared in a :class:`KeyConfig`.

    Steps are applied in this order:
    1. Validate business key columns exist.
    2. Compute surrogate key (if configured).
    3. Compute change detection hash (if configured).

    Args:
        df: Input DataFrame.
        keys: Key configuration — business key columns, surrogate key spec,
            change detection spec.

    Returns:
        DataFrame with surrogate key and/or change detection hash columns appended.

    Raises:
        ExecutionError: If any declared business key column is missing from ``df``.
    """
    result = df

    if keys.business_key:
        missing = set(keys.business_key) - set(df.columns)
        if missing:
            raise ExecutionError(f"Business key columns not found in DataFrame: {sorted(missing)}")

    if keys.surrogate_key is not None and keys.business_key is not None:
        result = _compute_surrogate_key(result, keys.surrogate_key, keys.business_key)

    if keys.change_detection is not None:
        result = _compute_change_hash(result, keys.change_detection)

    return result


def _compute_surrogate_key(
    df: DataFrame,
    config: SurrogateKeyConfig,
    business_key_columns: list[str],
) -> DataFrame:
    """Add a surrogate key column derived by hashing the business key columns.

    Null values in business key columns are replaced with the sentinel string
    ``__NULL__`` before hashing, so that null-containing keys produce a
    consistent, non-null hash rather than a null output.

    Args:
        df: Input DataFrame.
        config: Surrogate key configuration — output column name and hash algorithm.
        business_key_columns: Column names that form the business key.

    Returns:
        DataFrame with the surrogate key column appended.
    """
    concat_expr = _build_concat_expr(business_key_columns)
    hash_col = _apply_hash(concat_expr, config.algorithm, config.output)
    return df.withColumn(config.name, hash_col)


def _compute_change_hash(df: DataFrame, config: ChangeDetectionConfig) -> DataFrame:
    """Add a change detection hash column derived by hashing the specified columns.

    Null values are replaced with the sentinel string before hashing, ensuring
    the hash is always non-null and changes only when data values change.

    Args:
        df: Input DataFrame.
        config: Change detection configuration — output column name, column list,
            and hash algorithm.

    Returns:
        DataFrame with the change detection hash column appended.
    """
    concat_expr = _build_concat_expr(config.columns)
    hash_col = _apply_hash(concat_expr, config.algorithm, config.output)
    return df.withColumn(config.name, hash_col)


def _build_concat_expr(columns: list[str]):  # type: ignore[return]
    """Build a Spark Column that concatenates the named columns with a separator.

    Each column is cast to string and null values are replaced with the sentinel
    before concatenation.
    """
    coerced = [F.coalesce(F.col(c).cast("string"), F.lit(_NULL_SENTINEL)) for c in columns]
    return F.concat_ws(_KEY_SEPARATOR, *coerced)


def _apply_hash(col_expr, algorithm: str, output: str = "native"):  # type: ignore[return]
    """Apply the specified hash algorithm to a Column expression.

    Note:
        ``crc32`` has a 32-bit output space with higher collision risk.
        ``murmur3`` (Spark's ``hash()``) may vary across Spark major versions.
        Neither is recommended for high-cardinality surrogate keys.
    """
    if algorithm == "xxhash64":
        result = F.xxhash64(col_expr)
    elif algorithm == "sha1":
        result = F.sha1(col_expr)
    elif algorithm == "sha256":
        result = F.sha2(col_expr, 256)
    elif algorithm == "sha384":
        result = F.sha2(col_expr, 384)
    elif algorithm == "sha512":
        result = F.sha2(col_expr, 512)
    elif algorithm == "md5":
        result = F.md5(col_expr)
    elif algorithm == "crc32":
        result = F.crc32(col_expr)
    elif algorithm == "murmur3":
        result = F.hash(col_expr)
    else:
        raise ExecutionError(f"Unsupported hash algorithm: '{algorithm}'")

    if output == "string":
        result = result.cast("string")

    return result
