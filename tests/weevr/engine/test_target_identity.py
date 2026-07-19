"""Canonical target identity across the three declaration forms."""

import pytest
from pyspark.sql import SparkSession

from weevr.engine.target_identity import resolve_target_identity
from weevr.model.connection import OneLakeConnection

pytestmark = pytest.mark.spark


class TestIdentityEquality:
    def test_path_and_trailing_slash_agree(self, spark: SparkSession, tmp_path) -> None:
        p = str(tmp_path / "tid_tbl")
        a = resolve_target_identity(spark, path=p)
        b = resolve_target_identity(spark, path=p + "/")
        assert a == b

    def test_alias_and_path_agree_on_location(self, spark: SparkSession, tmp_path) -> None:
        p = str(tmp_path / "tid_alias_tbl")
        spark.createDataFrame([{"id": 1}]).write.format("delta").save(p)
        spark.sql(f"CREATE TABLE IF NOT EXISTS tid_alias USING DELTA LOCATION '{p}'")
        try:
            by_alias = resolve_target_identity(spark, alias="tid_alias")
            by_path = resolve_target_identity(spark, path=p)
            assert by_alias is not None and by_path is not None
            # Alias resolves to the catalog location; both keys name one table
            assert by_alias.rstrip("/").endswith(by_path.rsplit("/", 1)[-1])
            # Case-variant references to the SAME table meet on one key
            # (the metastore is case-insensitive, so both resolve to the
            # same location)
            assert by_alias == resolve_target_identity(spark, alias="TID_ALIAS")
        finally:
            spark.sql("DROP TABLE IF EXISTS tid_alias")

    def test_connection_form_matches_its_path(self, spark: SparkSession) -> None:
        conn = OneLakeConnection(
            type="onelake", workspace="ws-guid", lakehouse="lh-guid", default_schema="dbo"
        )
        ident = resolve_target_identity(
            spark,
            connection="main_lake",
            table="dim_customer",
            schema_override=None,
            connections={"main_lake": conn},
        )
        assert ident is not None
        assert ident.startswith("abfss://")
        assert ident.endswith("dim_customer")

    def test_unresolvable_returns_none(self, spark: SparkSession) -> None:
        assert resolve_target_identity(spark, connection="ghost", table="t") is None
        assert resolve_target_identity(spark) is None


class TestFallbackNamespace:
    def test_unresolved_alias_fallback_cannot_collide_with_locations(
        self, spark: SparkSession
    ) -> None:
        # No such table: the fallback key carries the alias: namespace so it
        # can never equal any location-derived key
        ident = resolve_target_identity(spark, alias="ghost.schema_table")
        assert ident == "alias:ghost.schema_table"
        assert not ident.startswith("/") and "://" not in ident
