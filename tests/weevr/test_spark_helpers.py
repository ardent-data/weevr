"""Tests for the shared Spark test helpers."""

import sys

import pyspark
import pytest
from pyspark.sql import DataFrame

from spark_helpers import dataframe_class

_CLASSIC = "pyspark.sql.classic.dataframe"


class TestDataframeClass:
    """``dataframe_class()`` must return the class sessions actually run."""

    def test_spark3_falls_back_to_the_single_class(self, monkeypatch):
        monkeypatch.setattr(pyspark, "__version__", "3.5.5")
        monkeypatch.setitem(sys.modules, _CLASSIC, None)
        assert dataframe_class() is DataFrame

    def test_spark4_without_classic_class_raises(self, monkeypatch):
        monkeypatch.setattr(pyspark, "__version__", "4.1.1")
        monkeypatch.setitem(sys.modules, _CLASSIC, None)
        with pytest.raises(ImportError):
            dataframe_class()

    @pytest.mark.spark
    def test_matches_the_session_dataframe(self, spark):
        assert dataframe_class() is type(spark.range(1))
