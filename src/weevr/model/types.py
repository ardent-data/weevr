"""Shared type aliases for the weevr domain model."""

from typing import NewType

SparkExpr = NewType("SparkExpr", str)
"""A Spark SQL expression string.

Used to annotate fields that hold Spark SQL expressions (e.g., filter predicates,
derived column expressions). At runtime these are plain strings; the NewType signals
intent and allows downstream type-checkers to distinguish expressions from arbitrary
strings.
"""
