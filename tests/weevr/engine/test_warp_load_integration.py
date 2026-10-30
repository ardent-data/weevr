"""Warps resolved at load reach the executor, end to end through ``Context.run``."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from pyspark.sql import SparkSession

from weevr.context import Context

pytestmark = pytest.mark.spark

WARP = """config_version: "1.0"
columns:
  - name: id
    type: bigint
  - name: name
    type: string
  - name: region
    type: string
    default: "n/a"
"""


def _write(path: Path, body: dict[str, Any] | str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body if isinstance(body, str) else json.dumps(body))


def _thread(src: str, target: dict[str, Any]) -> dict[str, Any]:
    return {
        "config_version": "1.0",
        "sources": {"main": {"type": "delta", "alias": src}},
        "target": target,
        "write": {"mode": "append"},
    }


class TestAliasTargetWarp:
    """The documented alias + warp config applies the warp, whatever the cwd."""

    def test_alias_target_with_explicit_warp(
        self,
        spark: SparkSession,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        request: pytest.FixtureRequest,
    ):
        src = str(tmp_path / "src")
        spark.createDataFrame([(1, "a")], ["id", "name"]).write.format("delta").save(src)
        # Own location so the database never lands in the default warehouse
        spark.sql(f"CREATE DATABASE b028_warp LOCATION '{tmp_path / 'b028_warp.db'}'")
        request.addfinalizer(lambda: spark.sql("DROP DATABASE IF EXISTS b028_warp CASCADE"))
        # Alias overwrite is unavailable on OSS Spark >= 3.5.6, so pre-create and append
        spark.sql(
            "CREATE TABLE b028_warp.dim_customer (id BIGINT, name STRING, region STRING) "
            "USING delta"
        )
        project = tmp_path / "p.weevr"
        _write(project / "threads" / "dim_customer.warp", WARP)
        _write(
            project / "threads" / "dim_customer.thread",
            _thread(src, {"alias": "b028_warp.dim_customer", "warp": "dim_customer"}),
        )
        monkeypatch.chdir(tmp_path)  # neither the project nor the thread directory

        result = Context(spark=spark, project=str(project)).run("threads/dim_customer.thread")

        assert result.status == "success", result.detail
        rows = spark.table("b028_warp.dim_customer").collect()
        assert [(r.id, r.name, r.region) for r in rows] == [(1, "a", "n/a")]


class TestWarpDiscovery:
    """A warp named after the target applies without target.warp."""

    def test_discovered_warp_appends_warp_only_column(self, spark: SparkSession, tmp_path: Path):
        src = str(tmp_path / "src")
        spark.createDataFrame([(1, "a")], ["id", "name"]).write.format("delta").save(src)
        target = str(tmp_path / "Tables" / "dim_customer")
        project = tmp_path / "p.weevr"
        _write(project / "dim_customer.warp", WARP)
        thread = _thread(src, {"path": target})
        thread["write"] = {"mode": "overwrite"}
        _write(project / "dim_customer.thread", thread)

        result = Context(spark=spark, project=str(project)).run("dim_customer.thread")

        assert result.status == "success", result.detail
        rows = spark.read.format("delta").load(target).collect()
        assert [(r.id, r.name, r.region) for r in rows] == [(1, "a", "n/a")]

    def test_opt_out_ignores_matching_warp(self, spark: SparkSession, tmp_path: Path):
        src = str(tmp_path / "src")
        spark.createDataFrame([(1, "a")], ["id", "name"]).write.format("delta").save(src)
        target = str(tmp_path / "Tables" / "dim_customer")
        project = tmp_path / "p.weevr"
        _write(project / "dim_customer.warp", WARP)
        thread = _thread(src, {"path": target, "warp": False})
        thread["write"] = {"mode": "overwrite"}
        _write(project / "dim_customer.thread", thread)

        result = Context(spark=spark, project=str(project)).run("dim_customer.thread")

        assert result.status == "success", result.detail
        assert spark.read.format("delta").load(target).columns == ["id", "name"]
