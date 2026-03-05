"""Spark fixtures for weevr tests."""

import os
import sys
from collections.abc import Generator
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

# Ensure PySpark worker processes use the same Python interpreter as the test
# runner (i.e., the virtual environment Python, not a system Python).
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    """Session-scoped SparkSession with Delta Lake support.

    Configures a local Spark session suitable for unit and integration tests.
    Uses configure_spark_with_delta_pip to handle Delta JAR loading automatically.
    """
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.master("local[2]")  # type: ignore[attr-defined]
        .appName("weevr-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture()
def tmp_delta_path(tmp_path: Path):
    """Function-scoped factory for isolated Delta table paths.

    Returns a callable that produces unique subdirectory paths under tmp_path,
    ensuring each test gets its own isolated Delta table location.
    """

    def _make_path(name: str) -> str:
        table_path = tmp_path / name
        table_path.mkdir(parents=True, exist_ok=True)
        return str(table_path)

    return _make_path
