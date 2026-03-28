"""Tests for hashing operations — surrogate key and change detection."""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

from weevr.errors.exceptions import ExecutionError
from weevr.model.dimension import AdditionalKeyConfig, ChangeDetectionGroupConfig, DimensionConfig
from weevr.model.dimension import SurrogateKeyConfig as DimensionSurrogateKeyConfig
from weevr.model.keys import ChangeDetectionConfig, KeyConfig, SurrogateKeyConfig
from weevr.operations.hashing import (
    compute_additional_keys,
    compute_dimension_keys,
    compute_keys,
    compute_named_group_hashes,
    resolve_auto_columns,
)

pytestmark = pytest.mark.spark


@pytest.fixture()
def people_df(spark: SparkSession):
    """DataFrame with name and department columns for key tests."""
    return spark.createDataFrame(
        [
            {"first": "alice", "last": "smith", "dept": "eng"},
            {"first": "bob", "last": "jones", "dept": "ops"},
            {"first": "carol", "last": "smith", "dept": "eng"},
        ]
    )


class TestSurrogateKey:
    """Tests for surrogate key generation via compute_keys."""

    def test_surrogate_key_sha256_added(self, people_df) -> None:
        keys = KeyConfig(
            business_key=["first", "last"],
            surrogate_key=SurrogateKeyConfig(name="sk", algorithm="sha256"),
        )
        result = compute_keys(people_df, keys)
        assert "sk" in result.columns
        assert result.count() == 3

    def test_surrogate_key_md5_added(self, people_df) -> None:
        keys = KeyConfig(
            business_key=["first", "last"],
            surrogate_key=SurrogateKeyConfig(name="sk_md5", algorithm="md5"),
        )
        result = compute_keys(people_df, keys)
        assert "sk_md5" in result.columns

    def test_surrogate_key_is_non_null(self, people_df) -> None:
        keys = KeyConfig(
            business_key=["first", "last"],
            surrogate_key=SurrogateKeyConfig(name="sk"),
        )
        result = compute_keys(people_df, keys)
        null_count = result.filter("sk IS NULL").count()
        assert null_count == 0

    def test_surrogate_key_is_same_length(self, people_df) -> None:
        keys = KeyConfig(
            business_key=["first", "last"],
            surrogate_key=SurrogateKeyConfig(name="sk", algorithm="sha256"),
        )
        result = compute_keys(people_df, keys)
        lengths = {len(r["sk"]) for r in result.collect()}
        # SHA-256 hex digest is always 64 chars
        assert lengths == {64}

    def test_surrogate_key_md5_length(self, people_df) -> None:
        keys = KeyConfig(
            business_key=["first", "last"],
            surrogate_key=SurrogateKeyConfig(name="sk", algorithm="md5"),
        )
        result = compute_keys(people_df, keys)
        lengths = {len(r["sk"]) for r in result.collect()}
        # MD5 hex digest is always 32 chars
        assert lengths == {32}

    def test_same_key_columns_produce_same_hash(self, spark: SparkSession) -> None:
        df = spark.createDataFrame(
            [
                {"id": 1, "k": "abc"},
                {"id": 2, "k": "abc"},
            ]
        )
        keys = KeyConfig(
            business_key=["k"],
            surrogate_key=SurrogateKeyConfig(name="sk"),
        )
        result = compute_keys(df, keys)
        hashes = {r["sk"] for r in result.collect()}
        assert len(hashes) == 1

    def test_different_key_columns_produce_different_hashes(self, people_df) -> None:
        keys = KeyConfig(
            business_key=["first", "last"],
            surrogate_key=SurrogateKeyConfig(name="sk"),
        )
        result = compute_keys(people_df, keys)
        hashes = {r["sk"] for r in result.collect()}
        assert len(hashes) == 3

    def test_null_business_key_produces_consistent_hash(self, spark: SparkSession) -> None:
        schema = StructType(
            [
                StructField("k", StringType(), nullable=True),
                StructField("v", LongType()),
            ]
        )
        df = spark.createDataFrame([(None, 1), (None, 2)], schema=schema)
        keys = KeyConfig(
            business_key=["k"],
            surrogate_key=SurrogateKeyConfig(name="sk"),
        )
        result = compute_keys(df, keys)
        # Both null key rows produce the same hash (sentinel applied)
        hashes = {r["sk"] for r in result.collect()}
        assert len(hashes) == 1
        # Hash must be non-null
        assert None not in hashes

    def test_surrogate_key_xxhash64_added(self, people_df) -> None:
        keys = KeyConfig(
            business_key=["first", "last"],
            surrogate_key=SurrogateKeyConfig(name="sk", algorithm="xxhash64"),
        )
        result = compute_keys(people_df, keys)
        assert "sk" in result.columns
        assert result.schema["sk"].dataType == LongType()

    def test_surrogate_key_xxhash64_string_output(self, people_df) -> None:
        keys = KeyConfig(
            business_key=["first", "last"],
            surrogate_key=SurrogateKeyConfig(name="sk", algorithm="xxhash64", output="string"),
        )
        result = compute_keys(people_df, keys)
        assert result.schema["sk"].dataType == StringType()

    def test_surrogate_key_sha1_added(self, people_df) -> None:
        keys = KeyConfig(
            business_key=["first", "last"],
            surrogate_key=SurrogateKeyConfig(name="sk", algorithm="sha1"),
        )
        result = compute_keys(people_df, keys)
        assert "sk" in result.columns
        lengths = {len(r["sk"]) for r in result.collect()}
        assert lengths == {40}  # SHA-1 hex digest is 40 chars

    def test_surrogate_key_sha384_added(self, people_df) -> None:
        keys = KeyConfig(
            business_key=["first", "last"],
            surrogate_key=SurrogateKeyConfig(name="sk", algorithm="sha384"),
        )
        result = compute_keys(people_df, keys)
        assert "sk" in result.columns
        lengths = {len(r["sk"]) for r in result.collect()}
        assert lengths == {96}  # SHA-384 hex digest is 96 chars

    def test_surrogate_key_sha512_added(self, people_df) -> None:
        keys = KeyConfig(
            business_key=["first", "last"],
            surrogate_key=SurrogateKeyConfig(name="sk", algorithm="sha512"),
        )
        result = compute_keys(people_df, keys)
        assert "sk" in result.columns
        lengths = {len(r["sk"]) for r in result.collect()}
        assert lengths == {128}  # SHA-512 hex digest is 128 chars

    def test_surrogate_key_crc32_added(self, people_df) -> None:
        keys = KeyConfig(
            business_key=["first", "last"],
            surrogate_key=SurrogateKeyConfig(name="sk", algorithm="crc32"),
        )
        result = compute_keys(people_df, keys)
        assert "sk" in result.columns
        assert result.schema["sk"].dataType == LongType()

    def test_surrogate_key_crc32_string_output(self, people_df) -> None:
        keys = KeyConfig(
            business_key=["first", "last"],
            surrogate_key=SurrogateKeyConfig(name="sk", algorithm="crc32", output="string"),
        )
        result = compute_keys(people_df, keys)
        assert result.schema["sk"].dataType == StringType()

    def test_surrogate_key_murmur3_added(self, people_df) -> None:
        keys = KeyConfig(
            business_key=["first", "last"],
            surrogate_key=SurrogateKeyConfig(name="sk", algorithm="murmur3"),
        )
        result = compute_keys(people_df, keys)
        assert "sk" in result.columns
        assert result.schema["sk"].dataType == IntegerType()

    def test_surrogate_key_murmur3_string_output(self, people_df) -> None:
        keys = KeyConfig(
            business_key=["first", "last"],
            surrogate_key=SurrogateKeyConfig(name="sk", algorithm="murmur3", output="string"),
        )
        result = compute_keys(people_df, keys)
        assert result.schema["sk"].dataType == StringType()


class TestChangeDetection:
    """Tests for change detection hash via compute_keys."""

    def test_change_hash_md5_added(self, people_df) -> None:
        keys = KeyConfig(
            change_detection=ChangeDetectionConfig(
                name="row_hash",
                columns=["first", "last", "dept"],
                algorithm="md5",
            )
        )
        result = compute_keys(people_df, keys)
        assert "row_hash" in result.columns
        assert result.count() == 3

    def test_change_hash_sha256_added(self, people_df) -> None:
        keys = KeyConfig(
            change_detection=ChangeDetectionConfig(
                name="row_hash",
                columns=["dept"],
                algorithm="sha256",
            )
        )
        result = compute_keys(people_df, keys)
        assert "row_hash" in result.columns

    def test_change_hash_same_rows_produce_same_hash(self, spark: SparkSession) -> None:
        df = spark.createDataFrame(
            [
                {"a": "x", "b": "y"},
                {"a": "x", "b": "y"},
            ]
        )
        keys = KeyConfig(change_detection=ChangeDetectionConfig(name="ch", columns=["a", "b"]))
        result = compute_keys(df, keys)
        hashes = {r["ch"] for r in result.collect()}
        assert len(hashes) == 1

    def test_change_hash_different_rows_produce_different_hashes(self, people_df) -> None:
        keys = KeyConfig(
            change_detection=ChangeDetectionConfig(name="ch", columns=["first", "last", "dept"])
        )
        result = compute_keys(people_df, keys)
        hashes = {r["ch"] for r in result.collect()}
        assert len(hashes) == 3

    def test_change_hash_null_column_produces_consistent_hash(self, spark: SparkSession) -> None:
        schema = StructType(
            [
                StructField("a", StringType(), nullable=True),
                StructField("b", StringType()),
            ]
        )
        df = spark.createDataFrame([(None, "x"), (None, "x")], schema=schema)
        keys = KeyConfig(change_detection=ChangeDetectionConfig(name="ch", columns=["a", "b"]))
        result = compute_keys(df, keys)
        hashes = {r["ch"] for r in result.collect()}
        assert len(hashes) == 1
        assert None not in hashes

    def test_change_hash_subset_of_columns(self, people_df) -> None:
        keys = KeyConfig(change_detection=ChangeDetectionConfig(name="dept_hash", columns=["dept"]))
        result = compute_keys(people_df, keys)
        # alice and carol are both in "eng" → same dept hash
        dept_hashes = {r["dept"]: r["dept_hash"] for r in result.collect()}
        assert dept_hashes["eng"] == dept_hashes["eng"]
        assert dept_hashes["eng"] != dept_hashes["ops"]

    def test_change_hash_xxhash64(self, people_df) -> None:
        keys = KeyConfig(
            change_detection=ChangeDetectionConfig(
                name="ch", columns=["first", "dept"], algorithm="xxhash64"
            )
        )
        result = compute_keys(people_df, keys)
        assert "ch" in result.columns
        assert result.schema["ch"].dataType == LongType()

    def test_change_hash_xxhash64_string_output(self, people_df) -> None:
        keys = KeyConfig(
            change_detection=ChangeDetectionConfig(
                name="ch", columns=["first", "dept"], algorithm="xxhash64", output="string"
            )
        )
        result = compute_keys(people_df, keys)
        assert result.schema["ch"].dataType == StringType()

    def test_change_hash_sha1(self, people_df) -> None:
        keys = KeyConfig(
            change_detection=ChangeDetectionConfig(
                name="ch", columns=["first", "dept"], algorithm="sha1"
            )
        )
        result = compute_keys(people_df, keys)
        assert "ch" in result.columns
        lengths = {len(r["ch"]) for r in result.collect()}
        assert lengths == {40}

    def test_change_hash_sha384(self, people_df) -> None:
        keys = KeyConfig(
            change_detection=ChangeDetectionConfig(
                name="ch", columns=["first", "dept"], algorithm="sha384"
            )
        )
        result = compute_keys(people_df, keys)
        assert "ch" in result.columns
        lengths = {len(r["ch"]) for r in result.collect()}
        assert lengths == {96}

    def test_change_hash_sha512(self, people_df) -> None:
        keys = KeyConfig(
            change_detection=ChangeDetectionConfig(
                name="ch", columns=["first", "dept"], algorithm="sha512"
            )
        )
        result = compute_keys(people_df, keys)
        assert "ch" in result.columns
        lengths = {len(r["ch"]) for r in result.collect()}
        assert lengths == {128}

    def test_change_hash_crc32(self, people_df) -> None:
        keys = KeyConfig(
            change_detection=ChangeDetectionConfig(name="ch", columns=["dept"], algorithm="crc32")
        )
        result = compute_keys(people_df, keys)
        assert result.schema["ch"].dataType == LongType()

    def test_change_hash_crc32_string_output(self, people_df) -> None:
        keys = KeyConfig(
            change_detection=ChangeDetectionConfig(
                name="ch", columns=["dept"], algorithm="crc32", output="string"
            )
        )
        result = compute_keys(people_df, keys)
        assert result.schema["ch"].dataType == StringType()

    def test_change_hash_murmur3(self, people_df) -> None:
        keys = KeyConfig(
            change_detection=ChangeDetectionConfig(name="ch", columns=["dept"], algorithm="murmur3")
        )
        result = compute_keys(people_df, keys)
        assert result.schema["ch"].dataType == IntegerType()

    def test_change_hash_murmur3_string_output(self, people_df) -> None:
        keys = KeyConfig(
            change_detection=ChangeDetectionConfig(
                name="ch", columns=["dept"], algorithm="murmur3", output="string"
            )
        )
        result = compute_keys(people_df, keys)
        assert result.schema["ch"].dataType == StringType()


class TestComputeKeysCombined:
    """Tests for combined key configs."""

    def test_surrogate_and_change_detection_together(self, people_df) -> None:
        keys = KeyConfig(
            business_key=["first", "last"],
            surrogate_key=SurrogateKeyConfig(name="sk"),
            change_detection=ChangeDetectionConfig(name="ch", columns=["dept"]),
        )
        result = compute_keys(people_df, keys)
        assert "sk" in result.columns
        assert "ch" in result.columns
        assert result.count() == 3

    def test_business_key_only_does_not_add_columns(self, people_df) -> None:
        keys = KeyConfig(business_key=["first", "last"])
        result = compute_keys(people_df, keys)
        assert set(result.columns) == set(people_df.columns)

    def test_missing_business_key_column_raises_execution_error(self, people_df) -> None:
        keys = KeyConfig(
            business_key=["first", "nonexistent"],
            surrogate_key=SurrogateKeyConfig(name="sk"),
        )
        with pytest.raises(ExecutionError, match="nonexistent"):
            compute_keys(people_df, keys)

    def test_all_missing_columns_listed_in_error(self, people_df) -> None:
        keys = KeyConfig(business_key=["missing_a", "missing_b"])
        with pytest.raises(ExecutionError) as exc_info:
            compute_keys(people_df, keys)
        assert "missing_a" in str(exc_info.value)
        assert "missing_b" in str(exc_info.value)

    def test_xxhash64_surrogate_with_md5_change_detection(self, people_df) -> None:
        keys = KeyConfig(
            business_key=["first", "last"],
            surrogate_key=SurrogateKeyConfig(name="sk", algorithm="xxhash64"),
            change_detection=ChangeDetectionConfig(name="ch", columns=["dept"], algorithm="md5"),
        )
        result = compute_keys(people_df, keys)
        assert result.schema["sk"].dataType == LongType()
        assert result.schema["ch"].dataType == StringType()
        assert result.count() == 3


class TestResolveAutoColumns:
    """Tests for resolve_auto_columns utility."""

    def test_returns_remaining_columns_sorted(self) -> None:
        all_cols = ["z_col", "a_col", "m_col", "sk", "bk"]
        exclude = {"sk", "bk"}
        result = resolve_auto_columns(all_cols, exclude)
        assert result == ["a_col", "m_col", "z_col"]

    def test_empty_exclude_returns_all_sorted(self) -> None:
        all_cols = ["c", "a", "b"]
        result = resolve_auto_columns(all_cols, set())
        assert result == ["a", "b", "c"]

    def test_exclude_all_returns_empty(self) -> None:
        all_cols = ["a", "b"]
        result = resolve_auto_columns(all_cols, {"a", "b"})
        assert result == []

    def test_exclude_set_with_unknown_columns_is_safe(self) -> None:
        # Columns in exclude_set that don't exist in df_columns are silently ignored
        all_cols = ["a", "b", "c"]
        result = resolve_auto_columns(all_cols, {"d", "e", "b"})
        assert result == ["a", "c"]


class TestComputeNamedGroupHashes:
    """Tests for compute_named_group_hashes."""

    def test_named_group_adds_hash_column(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([{"a": "x", "b": "y"}])
        groups = {
            "core": ChangeDetectionGroupConfig(
                name="core_hash",
                columns=["a", "b"],
                on_change="overwrite",
                algorithm="sha256",
            )
        }
        result = compute_named_group_hashes(df, groups, exclude_columns=set())
        assert "core_hash" in result.columns

    def test_group_dict_key_used_when_name_is_none(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([{"a": "x", "b": "y"}])
        groups = {
            "my_group": ChangeDetectionGroupConfig(
                columns=["a"],
                on_change="overwrite",
                algorithm="sha256",
            )
        }
        result = compute_named_group_hashes(df, groups, exclude_columns=set())
        assert "my_group" in result.columns

    def test_static_group_with_no_name_and_no_algorithm_produces_no_column(
        self, spark: SparkSession
    ) -> None:
        df = spark.createDataFrame([{"a": "x", "b": "y"}])
        groups = {
            "_static": ChangeDetectionGroupConfig(
                columns=["b"],
                on_change="static",
                # name=None, algorithm=None — DEC-012: skip hash computation
            )
        }
        result = compute_named_group_hashes(df, groups, exclude_columns=set())
        # No new columns should be added for a static group with no name/algorithm
        assert set(result.columns) == {"a", "b"}

    def test_auto_columns_resolved_with_exclude(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([{"id": 1, "val": "x", "sk": "abc"}])
        groups = {
            "_default": ChangeDetectionGroupConfig(
                name="_row_hash",
                columns="auto",
                on_change="overwrite",
                algorithm="sha256",
            )
        }
        result = compute_named_group_hashes(df, groups, exclude_columns={"id", "sk"})
        assert "_row_hash" in result.columns

    def test_default_algorithm_is_sha256_when_none(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([{"a": "x"}])
        groups = {
            "g": ChangeDetectionGroupConfig(
                name="g_hash",
                columns=["a"],
                on_change="overwrite",
                algorithm=None,
            )
        }
        result = compute_named_group_hashes(df, groups, exclude_columns=set())
        assert "g_hash" in result.columns
        # sha256 produces 64-char hex
        lengths = {len(r["g_hash"]) for r in result.collect()}
        assert lengths == {64}

    def test_multiple_groups_each_get_own_column(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([{"a": "x", "b": "y", "c": "z"}])
        groups = {
            "group1": ChangeDetectionGroupConfig(
                name="hash1",
                columns=["a"],
                on_change="overwrite",
                algorithm="sha256",
            ),
            "group2": ChangeDetectionGroupConfig(
                name="hash2",
                columns=["b", "c"],
                on_change="overwrite",
                algorithm="md5",
            ),
        }
        result = compute_named_group_hashes(df, groups, exclude_columns=set())
        assert "hash1" in result.columns
        assert "hash2" in result.columns


class TestComputeAdditionalKeys:
    """Tests for compute_additional_keys."""

    def test_additional_key_column_added(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([{"region": "EU", "category": "tech"}])
        additional_keys = {
            "region_key": AdditionalKeyConfig(
                name="region_sk",
                columns=["region", "category"],
                algorithm="sha256",
            )
        }
        result = compute_additional_keys(df, additional_keys)
        assert "region_sk" in result.columns

    def test_multiple_additional_keys(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([{"a": "x", "b": "y", "c": "z"}])
        additional_keys = {
            "key1": AdditionalKeyConfig(name="key1_hash", columns=["a"], algorithm="md5"),
            "key2": AdditionalKeyConfig(name="key2_hash", columns=["b", "c"], algorithm="sha256"),
        }
        result = compute_additional_keys(df, additional_keys)
        assert "key1_hash" in result.columns
        assert "key2_hash" in result.columns

    def test_additional_key_is_non_null(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([{"a": "x"}, {"a": "y"}])
        additional_keys = {
            "ak": AdditionalKeyConfig(name="ak_hash", columns=["a"], algorithm="sha256")
        }
        result = compute_additional_keys(df, additional_keys)
        null_count = result.filter("ak_hash IS NULL").count()
        assert null_count == 0


class TestComputeDimensionKeys:
    """Tests for compute_dimension_keys — the top-level dimension hashing function."""

    def _minimal_config(self) -> DimensionConfig:
        return DimensionConfig(
            business_key=["id"],
            surrogate_key=DimensionSurrogateKeyConfig(name="sk", columns=["id"]),
        )

    def test_minimal_config_adds_sk_and_default_row_hash(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}])
        config = self._minimal_config()
        result = compute_dimension_keys(df, config)
        assert "sk" in result.columns
        assert "_row_hash" in result.columns
        assert result.count() == 2

    def test_surrogate_key_is_non_null(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([{"id": 1, "name": "alice"}])
        config = self._minimal_config()
        result = compute_dimension_keys(df, config)
        null_count = result.filter("sk IS NULL").count()
        assert null_count == 0

    def test_named_groups_each_produce_separate_hash_column(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([{"id": 1, "name": "alice", "dept": "eng", "status": "A"}])
        config = DimensionConfig(
            business_key=["id"],
            surrogate_key=DimensionSurrogateKeyConfig(name="sk", columns=["id"]),
            change_detection={
                "core": ChangeDetectionGroupConfig(
                    name="core_hash",
                    columns=["name", "dept"],
                    on_change="overwrite",
                    algorithm="sha256",
                ),
                "meta": ChangeDetectionGroupConfig(
                    name="meta_hash",
                    columns=["status"],
                    on_change="static",
                    algorithm="md5",
                ),
            },
        )
        result = compute_dimension_keys(df, config)
        assert "sk" in result.columns
        assert "core_hash" in result.columns
        assert "meta_hash" in result.columns

    def test_auto_columns_exclude_key_and_sk_columns(self, spark: SparkSession) -> None:
        # 'id' is BK + SK source; 'sk' is SK output; '_row_hash' is auto group output
        # auto should resolve to 'name' only
        df = spark.createDataFrame([{"id": 1, "name": "alice"}])
        config = self._minimal_config()
        result = compute_dimension_keys(df, config)
        # Row hash column should exist and be non-null
        assert "_row_hash" in result.columns
        null_count = result.filter("_row_hash IS NULL").count()
        assert null_count == 0

    def test_additional_keys_are_computed(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([{"id": 1, "name": "alice", "region": "EU"}])
        config = DimensionConfig(
            business_key=["id"],
            surrogate_key=DimensionSurrogateKeyConfig(name="sk", columns=["id"]),
            additional_keys={
                "region_key": AdditionalKeyConfig(
                    name="region_sk",
                    columns=["region"],
                    algorithm="sha256",
                )
            },
        )
        result = compute_dimension_keys(df, config)
        assert "sk" in result.columns
        assert "region_sk" in result.columns

    def test_missing_business_key_column_raises(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([{"id": 1, "name": "alice"}])
        config = DimensionConfig(
            business_key=["id", "missing_col"],
            surrogate_key=DimensionSurrogateKeyConfig(name="sk", columns=["id"]),
        )
        with pytest.raises(ExecutionError, match="missing_col"):
            compute_dimension_keys(df, config)

    def test_audit_columns_excluded_from_auto_hash(self, spark: SparkSession) -> None:
        """Audit columns are excluded from auto group hash when passed."""
        df = spark.createDataFrame(
            [{"id": 1, "name": "alice", "_loaded_at": "2026-01-01", "_run_id": "abc"}]
        )
        config = self._minimal_config()
        # With audit columns: auto should resolve to 'name' only
        result_with = compute_dimension_keys(df, config, audit_columns={"_loaded_at", "_run_id"})
        # Without audit columns: auto resolves to 'name', '_loaded_at', '_run_id'
        result_without = compute_dimension_keys(df, config, audit_columns=None)
        # The hash values should differ because different columns are hashed
        hash_with = result_with.select("_row_hash").collect()[0][0]
        hash_without = result_without.select("_row_hash").collect()[0][0]
        assert hash_with != hash_without

    def test_empty_auto_raises_execution_error(self, spark: SparkSession) -> None:
        """auto resolves to empty set when all columns are engine-managed."""
        # DataFrame with only BK + SK source columns — nothing left for auto
        df = spark.createDataFrame([{"id": 1}])
        config = DimensionConfig(
            business_key=["id"],
            surrogate_key=DimensionSurrogateKeyConfig(name="sk", columns=["id"]),
        )
        with pytest.raises(ExecutionError, match="no data columns remain"):
            compute_dimension_keys(df, config)

    def test_existing_compute_keys_still_works(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([{"first": "alice", "last": "smith", "dept": "eng"}])
        keys = KeyConfig(
            business_key=["first", "last"],
            surrogate_key=SurrogateKeyConfig(name="sk"),
            change_detection=ChangeDetectionConfig(name="ch", columns=["dept"]),
        )
        result = compute_keys(df, keys)
        assert "sk" in result.columns
        assert "ch" in result.columns
