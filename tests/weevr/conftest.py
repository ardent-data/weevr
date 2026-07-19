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
def spark_action_counter(monkeypatch) -> dict[str, int]:
    """Count Spark actions triggered through DataFrame methods.

    Wraps every DataFrame method that launches a job (collect, count,
    first, take, toPandas) and counts outermost invocations only —
    first() delegates to take() which delegates to collect(), and the
    depth guard keeps that chain from counting as three actions.

    Returns a dict whose "total" key holds the running action count.
    """
    from pyspark.sql import DataFrame

    state = {"total": 0, "_depth": 0}

    def _make_wrapper(original):
        def wrapper(self, *args, **kwargs):
            state["_depth"] += 1
            if state["_depth"] == 1:
                state["total"] += 1
            try:
                return original(self, *args, **kwargs)
            finally:
                state["_depth"] -= 1

        return wrapper

    for method_name in ("collect", "count", "first", "take", "toPandas"):
        monkeypatch.setattr(DataFrame, method_name, _make_wrapper(getattr(DataFrame, method_name)))

    return state


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


@pytest.fixture()
def job_counter(spark: SparkSession):
    """Count Spark jobs executed inside a ``with`` block.

    Uses the status tracker's known-job-id set, so it counts real JVM
    jobs — including those launched without a Python-side action call
    (writes, merges, observation-fulfilling actions). Complements
    ``spark_action_counter``, which counts only DataFrame API calls.

    Usage::

        with job_counter() as jc:
            df.write.format("delta").save(path)
        assert jc.jobs == expected
    """

    class _JobCount:
        def __enter__(self):
            tracker = spark.sparkContext.statusTracker()
            self._before = set(tracker.getJobIdsForGroup(None))
            return self

        def __exit__(self, *exc: object) -> bool:
            tracker = spark.sparkContext.statusTracker()
            self._after = set(tracker.getJobIdsForGroup(None))
            return False

        @property
        def jobs(self) -> int:
            return len(self._after - self._before)

    return _JobCount
