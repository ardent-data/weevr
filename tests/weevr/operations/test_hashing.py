"""Tests for hashing operations — surrogate key and change detection."""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType

from weevr.errors.exceptions import ExecutionError
from weevr.model.keys import ChangeDetectionConfig, KeyConfig, SurrogateKeyConfig
from weevr.operations.hashing import compute_keys


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
