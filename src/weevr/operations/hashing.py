"""Hashing operations — surrogate key generation and change detection."""

import logging

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from weevr.errors.exceptions import ExecutionError
from weevr.model.dimension import (
    AdditionalKeyConfig,
    ChangeDetectionGroupConfig,
    DimensionConfig,
)
from weevr.model.keys import ChangeDetectionConfig, KeyConfig, SurrogateKeyConfig

logger = logging.getLogger(__name__)

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


def _build_concat_expr(columns: list[str]) -> Column:
    """Build a Spark Column that concatenates the named columns with a separator.

    Each column is cast to string and null values are replaced with the sentinel
    before concatenation.
    """
    coerced = [F.coalesce(F.col(c).cast("string"), F.lit(_NULL_SENTINEL)) for c in columns]
    return F.concat_ws(_KEY_SEPARATOR, *coerced)


def _apply_hash(col_expr: Column, algorithm: str, output: str = "native") -> Column:
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


# ---------------------------------------------------------------------------
# Dimension key computation
# ---------------------------------------------------------------------------


def compute_dimension_keys(
    df: DataFrame,
    dimension_config: DimensionConfig,
    audit_columns: set[str] | None = None,
) -> DataFrame:
    """Apply all key and hash computations declared in a :class:`DimensionConfig`.

    Steps are applied in this order:

    1. Validate that all business key columns exist in the DataFrame.
    2. Compute the primary surrogate key from ``dimension_config.surrogate_key``.
    3. Compute change detection hashes for each group in
       ``dimension_config.change_detection``.
    4. Compute additional keys from ``dimension_config.additional_keys`` (if any).

    Groups that declare ``columns: "auto"`` have their column list resolved at
    runtime by excluding the full set of engine-managed columns (business key,
    surrogate key source and output, additional key sources and outputs, SCD
    tracking columns, audit columns, previous_columns source columns, and
    columns claimed by other explicit groups).

    Args:
        df: Input DataFrame.
        dimension_config: Full dimension configuration — business key, surrogate
            key, change detection groups, additional keys, and SCD settings.
        audit_columns: Names of audit columns to exclude from ``auto`` hash
            computation. When ``None``, no audit columns are excluded.

    Returns:
        DataFrame with surrogate key, change detection hash, and additional key
        columns appended.

    Raises:
        ExecutionError: If any declared business key column is missing from ``df``.
    """
    result = df

    # 1. Validate business key columns.
    missing = set(dimension_config.business_key) - set(df.columns)
    if missing:
        raise ExecutionError(f"Business key columns not found in DataFrame: {sorted(missing)}")

    # 2. Compute the primary surrogate key.
    sk_cfg = dimension_config.surrogate_key
    concat_expr = _build_concat_expr(sk_cfg.columns)
    sk_col = _apply_hash(concat_expr, sk_cfg.algorithm, sk_cfg.output)
    result = result.withColumn(sk_cfg.name, sk_col)

    # 3. Build the exclusion set used for auto column resolution.
    exclude = _build_auto_exclusion_set(dimension_config, audit_columns)

    # 4. Compute change detection group hashes.
    if dimension_config.change_detection:
        result = compute_named_group_hashes(result, dimension_config.change_detection, exclude)

        # Log unassigned columns (those not in any explicit group and not
        # absorbed by an auto group) so users know what defaults to overwrite.
        has_auto = any(g.columns == "auto" for g in dimension_config.change_detection.values())
        if not has_auto:
            assigned = set()
            for g in dimension_config.change_detection.values():
                if isinstance(g.columns, list):
                    assigned.update(g.columns)
            unassigned = sorted(c for c in df.columns if c not in exclude and c not in assigned)
            if unassigned:
                logger.info(
                    "Unassigned columns default to overwrite: %s",
                    ", ".join(unassigned),
                )

    # 5. Compute additional keys.
    if dimension_config.additional_keys:
        result = compute_additional_keys(result, dimension_config.additional_keys)

    return result


def resolve_auto_columns(df_columns: list[str], exclude_set: set[str]) -> list[str]:
    """Return the subset of ``df_columns`` not in ``exclude_set``, sorted.

    Used to determine which columns belong to a change detection group that
    declares ``columns: "auto"``.

    Args:
        df_columns: All column names present in the DataFrame.
        exclude_set: Column names to exclude from the result.

    Returns:
        Sorted list of column names that remain after exclusion.
    """
    return sorted(c for c in df_columns if c not in exclude_set)


def compute_named_group_hashes(
    df: DataFrame,
    groups: dict[str, ChangeDetectionGroupConfig],
    exclude_columns: set[str],
) -> DataFrame:
    """Add one hash column per change detection group to the DataFrame.

    For each group in ``groups``:

    - The output column name is taken from ``group.name`` when set, otherwise
      the dict key is used.
    - When ``group.columns`` is ``"auto"``, the column list is resolved by
      calling :func:`resolve_auto_columns` with ``exclude_columns``.
    - The hash algorithm defaults to ``sha256`` when ``group.algorithm`` is
      ``None``.
    - A static group (``on_change: "static"``) with neither a name nor an
      algorithm is skipped — no hash column is produced (DEC-012).

    Args:
        df: Input DataFrame.
        groups: Mapping of group key → :class:`ChangeDetectionGroupConfig`.
        exclude_columns: Columns to omit when resolving ``columns: "auto"``.

    Returns:
        DataFrame with one hash column appended per qualifying group.
    """
    result = df

    for group_key, group in groups.items():
        # DEC-012: skip static groups that carry no name and no algorithm.
        if group.on_change == "static" and group.name is None and group.algorithm is None:
            continue

        output_col = group.name if group.name is not None else group_key
        algorithm = group.algorithm if group.algorithm is not None else "sha256"

        if group.columns == "auto":
            columns = resolve_auto_columns(list(result.columns), exclude_columns)
            if not columns:
                raise ExecutionError(
                    f"change_detection group '{group_key}' uses columns: auto "
                    f"but all columns are engine-managed — no data columns remain"
                )
        else:
            columns = list(group.columns)  # type: ignore[arg-type]

        concat_expr = _build_concat_expr(columns)
        hash_col = _apply_hash(concat_expr, algorithm, group.output)
        result = result.withColumn(output_col, hash_col)

    return result


def compute_additional_keys(
    df: DataFrame,
    additional_keys: dict[str, AdditionalKeyConfig],
) -> DataFrame:
    """Add one hash column per additional key configuration to the DataFrame.

    Each entry in ``additional_keys`` produces a column named ``config.name``
    by hashing the specified source columns with the configured algorithm.

    Args:
        df: Input DataFrame.
        additional_keys: Mapping of key label → :class:`AdditionalKeyConfig`.

    Returns:
        DataFrame with one hash column appended per additional key.
    """
    result = df
    for _key, config in additional_keys.items():
        concat_expr = _build_concat_expr(config.columns)
        hash_col = _apply_hash(concat_expr, config.algorithm, config.output)
        result = result.withColumn(config.name, hash_col)
    return result


def _build_auto_exclusion_set(
    config: DimensionConfig,
    audit_columns: set[str] | None = None,
) -> set[str]:
    """Build the full set of column names to exclude when resolving ``columns: "auto"``.

    The exclusion set covers all engine-managed columns so that ``auto`` groups
    only hash payload data columns:

    - Business key columns.
    - Surrogate key output column name.
    - Surrogate key source columns.
    - Additional key output column names and their source columns.
    - SCD tracking column names (valid_from, valid_to, is_current).
    - Columns explicitly listed in non-auto change detection groups.
    - Output column names of change detection groups (group.name or dict key).
    - previous_columns source column names (the dict values, per DEC-014).
    - previous_columns output column names (the dict keys).
    - Audit columns from the resolved audit config.

    Args:
        config: Full :class:`DimensionConfig`.
        audit_columns: Names of audit columns to exclude. When ``None``, no
            audit columns are excluded.

    Returns:
        Set of column names to pass to :func:`resolve_auto_columns`.
    """
    exclude: set[str] = set()

    # Business key and surrogate key.
    exclude.update(config.business_key)
    exclude.add(config.surrogate_key.name)
    exclude.update(config.surrogate_key.columns)

    # Additional key outputs and sources.
    if config.additional_keys:
        for ak in config.additional_keys.values():
            exclude.add(ak.name)
            exclude.update(ak.columns)

    # SCD tracking columns.
    exclude.add(config.columns.valid_from)
    exclude.add(config.columns.valid_to)
    exclude.add(config.columns.is_current)

    # Change detection group: columns and output names of non-auto groups.
    if config.change_detection:
        for group_key, group in config.change_detection.items():
            output_col = group.name if group.name is not None else group_key
            exclude.add(output_col)
            if group.columns != "auto":
                exclude.update(group.columns)  # type: ignore[arg-type]

    # previous_columns: exclude output column names (dict keys) since they
    # are engine-managed tracking columns. Source column names (dict values)
    # are NOT excluded — they must remain in the hash so changes to tracked
    # attributes trigger change detection.
    if config.previous_columns:
        exclude.update(config.previous_columns.keys())

    # Audit columns from resolved audit config.
    if audit_columns:
        exclude.update(audit_columns)

    return exclude
