"""Tests for shared Delta Lake utilities."""

import pytest

from weevr.delta import delta_table_exists, is_table_alias

pytestmark = pytest.mark.spark


class TestIsTableAlias:
    def test_dotted_name_is_alias(self):
        assert is_table_alias("staging.customers") is True

    def test_uri_is_not_alias(self):
        assert is_table_alias("abfss://ws@onelake/lh/Tables/t") is False

    def test_local_path_is_not_alias(self):
        assert is_table_alias("/tmp/tables/t") is False


class TestDeltaTableExists:
    """Equivalence matrix: alias form, path form, and missing table."""

    @pytest.fixture()
    def alias_table(self, spark):
        name = "default.exists_probe_present"
        spark.sql(f"DROP TABLE IF EXISTS {name}")
        spark.createDataFrame([(1, "a")], "id INT, val STRING").write.format("delta").saveAsTable(
            name
        )
        yield name
        spark.sql(f"DROP TABLE IF EXISTS {name}")

    def test_alias_present(self, spark, alias_table):
        assert delta_table_exists(spark, alias_table) is True

    def test_alias_missing(self, spark):
        assert delta_table_exists(spark, "default.exists_probe_absent") is False

    def test_path_present(self, spark, tmp_delta_path):
        path = tmp_delta_path("exists_path_present")
        spark.createDataFrame([(1,)], "id INT").write.format("delta").mode("overwrite").save(path)
        assert delta_table_exists(spark, path) is True

    def test_path_missing(self, spark, tmp_delta_path):
        path = tmp_delta_path("exists_path_absent")
        assert delta_table_exists(spark, path) is False

    def test_alias_probe_never_constructs_a_dataframe(self, spark, alias_table, monkeypatch):
        """Existence by alias is a metastore question.

        The job counter can't discriminate here — even the old
        ``limit(0).collect()`` probe completes without a JVM job. The
        cost being removed is the reader's Delta-log resolution, so the
        lock is that no DataFrameReader method ever runs.
        """
        from pyspark.sql.readwriter import DataFrameReader

        calls = {"n": 0}

        def _count(original):
            def wrapper(self, *args, **kwargs):
                calls["n"] += 1
                return original(self, *args, **kwargs)

            return wrapper

        monkeypatch.setattr(DataFrameReader, "table", _count(DataFrameReader.table))
        monkeypatch.setattr(DataFrameReader, "load", _count(DataFrameReader.load))
        assert delta_table_exists(spark, alias_table) is True
        assert delta_table_exists(spark, "default.exists_probe_absent") is False
        assert calls["n"] == 0

    def test_alias_probe_launches_zero_jobs(self, spark, alias_table, job_counter):
        with job_counter() as jc:
            assert delta_table_exists(spark, alias_table) is True
        assert jc.jobs == 0
