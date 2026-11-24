"""A lookup's relative file path reads from the project folder at run time."""

from __future__ import annotations

from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from documents import write_document
from weevr import Context


@pytest.mark.spark
def test_weave_lookup_reads_relative_csv_from_the_project(
    spark: SparkSession, tmp_path: Path
) -> None:
    """A weave lookup's relative csv path resolves under the project folder.

    A path left relative would be resolved by Spark against its own working
    directory (fixed when the session starts), which is never the project
    folder in ``tmp_path``.
    """
    project = tmp_path / "lookups.weevr"
    (project / "ref").mkdir(parents=True)
    (project / "ref" / "regions.csv").write_text("code,name\nN,North\nS,South\n")
    target = tmp_path / "lake" / "regions"
    write_document(
        project / "regions.thread",
        'config_version: "2.0"\n'
        "sources:\n  regions:\n    lookup: regions\n"
        f'target:\n  path: "{target}"\n',
    )
    write_document(
        project / "ref_data.weave",
        'config_version: "2.0"\n'
        "lookups:\n  regions:\n    source:\n      type: csv\n      path: ref/regions.csv\n"
        '      options:\n        header: "true"\n    materialize: false\n'
        "threads:\n  - ref: regions.thread\n",
    )
    result = Context(spark=spark, project=str(project)).run("ref_data.weave")

    assert result.status == "success", result.summary()
    rows = spark.read.format("delta").load(str(target)).orderBy("code").collect()
    assert [(r["code"], r["name"]) for r in rows] == [("N", "North"), ("S", "South")]
