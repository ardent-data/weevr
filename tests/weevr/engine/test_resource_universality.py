"""Integration tests for shared resource universality."""

from unittest.mock import patch

import pytest
from pyspark.sql import SparkSession

from spark_helpers import create_delta_table
from weevr.config.resolver import resolve_variables
from weevr.engine import build_plan, execute_loom, execute_weave
from weevr.engine.executor import execute_thread
from weevr.model.lookup import Lookup
from weevr.model.loom import Loom
from weevr.model.source import Source
from weevr.model.target import Target
from weevr.model.thread import Thread
from weevr.model.weave import ThreadEntry, Weave

# ---------------------------------------------------------------------------
# Workflow 4: Whole-value parameter resolution — pure Python, no Spark
# ---------------------------------------------------------------------------


class TestWholeValueIntegration:
    """Whole-value parameter resolution integration tests."""

    def test_match_keys_resolves_to_list(self):
        config = {"write": {"match_keys": "${pk_columns}"}}
        context = {"pk_columns": ["mandt", "color"]}
        result = resolve_variables(config, context)
        assert result["write"]["match_keys"] == ["mandt", "color"]
        assert isinstance(result["write"]["match_keys"], list)

    def test_business_key_resolves_to_list(self):
        config = {"keys": {"business_key": "${pk_columns}"}}
        context = {"pk_columns": ["mandt", "color"]}
        result = resolve_variables(config, context)
        assert result["keys"]["business_key"] == ["mandt", "color"]
        assert isinstance(result["keys"]["business_key"], list)

    def test_embedded_param_resolves_to_string(self):
        config = {"target": {"alias": "SAP.${table_name}"}}
        context = {"table_name": "MARA"}
        result = resolve_variables(config, context)
        assert result["target"]["alias"] == "SAP.MARA"
        assert isinstance(result["target"]["alias"], str)

    def test_whole_value_none_resolves_to_null(self):
        config = {"flag": "${opt}"}
        context = {"opt": None}
        result = resolve_variables(config, context)
        assert result["flag"] is None

    def test_existing_weave_config_resolves_identically(self):
        """Regression: existing weave-style configs with embedded params still work."""
        config = {
            "threads": ["dim_customer"],
            "params": {"env": {"type": "string"}},
            "defaults": {"tags": ["${env}"]},
        }
        context = {"env": "prod"}
        result = resolve_variables(config, context)
        assert result["defaults"]["tags"] == ["prod"]


# ---------------------------------------------------------------------------
# Workflows 1-3: Engine-level integration tests — Spark required
# ---------------------------------------------------------------------------


def _entries(*names: str) -> list[ThreadEntry]:
    return [ThreadEntry(name=n) for n in names]


@pytest.mark.spark
class TestResourceLifecycleIntegration:
    """Spark integration tests for resource lifecycle workflows."""

    # ------------------------------------------------------------------
    # Workflow 1: Standalone thread with inline lookup
    # ------------------------------------------------------------------

    def test_standalone_thread_with_inline_lookup(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        """Thread with an inline lookup resolves the source and executes successfully."""
        src = tmp_delta_path("rul_main_src")
        lookup_src = tmp_delta_path("rul_lookup_src")
        tgt = tmp_delta_path("rul_tgt")

        create_delta_table(spark, src, [{"id": 1, "val": "a"}])
        create_delta_table(spark, lookup_src, [{"ref_id": 10, "label": "x"}])

        thread = Thread(
            name="inline_lkp_thread",
            config_version="1",
            sources={
                "main": Source(type="delta", alias=src),
                "ref": Source(lookup="my_lookup"),
            },
            steps=[],
            target=Target(path=tgt),
            lookups={
                "my_lookup": Lookup(source=Source(type="delta", alias=lookup_src)),
            },
        )

        result = execute_thread(spark, thread)

        assert result.status == "success"
        assert result.rows_written == 1

    # ------------------------------------------------------------------
    # Workflow 2: Weave thread with lookup override
    # ------------------------------------------------------------------

    def test_weave_thread_with_lookup_override(self, spark: SparkSession, tmp_delta_path) -> None:
        """Thread-level lookup overrides the weave-level lookup of the same name."""
        weave_lookup_src = tmp_delta_path("rul_weave_lkp_src")
        thread_lookup_src = tmp_delta_path("rul_thread_lkp_src")
        main_src = tmp_delta_path("rul_override_main_src")
        tgt = tmp_delta_path("rul_override_tgt")

        create_delta_table(spark, weave_lookup_src, [{"id": 99, "origin": "weave"}])
        create_delta_table(
            spark, thread_lookup_src, [{"id": 1, "origin": "thread"}, {"id": 2, "origin": "thread"}]
        )
        create_delta_table(spark, main_src, [{"id": 1}])

        # Thread overrides the weave-level lookup_a with a different source
        thread = Thread(
            name="override_thread",
            config_version="1",
            sources={
                "main": Source(type="delta", alias=main_src),
                "ref": Source(lookup="lookup_a"),
            },
            steps=[],
            target=Target(path=tgt),
            lookups={
                "lookup_a": Lookup(source=Source(type="delta", alias=thread_lookup_src)),
            },
        )

        threads = {"override_thread": thread}
        plan = build_plan("override_weave", threads, _entries("override_thread"))

        # Weave-level lookup_a points to a different source
        weave_cached = {
            "lookup_a": spark.read.format("delta").load(weave_lookup_src),
        }
        weave_lookups = {
            "lookup_a": Lookup(source=Source(type="delta", alias=weave_lookup_src)),
        }

        result = execute_weave(
            spark,
            plan,
            threads,
            pre_cached_lookups=weave_cached,
            lookups=weave_lookups,
        )

        assert result.status == "success"
        assert len(result.thread_results) == 1
        assert result.thread_results[0].status == "success"
        # Thread uses main source (1 row) not the lookup as primary driver
        assert result.thread_results[0].rows_written == 1

    # ------------------------------------------------------------------
    # Workflow 3: Loom pre_steps and post_steps lifecycle
    # ------------------------------------------------------------------

    def test_loom_pre_and_post_steps_lifecycle(self, spark: SparkSession, tmp_delta_path) -> None:
        """Loom pre_steps and post_steps both execute around the weave run."""
        src = tmp_delta_path("rul_loom_src")
        tgt = tmp_delta_path("rul_loom_tgt")

        create_delta_table(spark, src, [{"id": 1}])

        loom = Loom.model_validate(
            {
                "name": "lifecycle_loom",
                "config_version": "1",
                "weaves": ["lifecycle_weave"],
                "pre_steps": [
                    {"type": "log_message", "message": "loom-pre-fired", "level": "info"}
                ],
                "post_steps": [
                    {"type": "log_message", "message": "loom-post-fired", "level": "info"}
                ],
            }
        )

        weave = Weave.model_validate(
            {"config_version": "1", "name": "lifecycle_weave", "threads": ["loom_thread"]}
        )

        thread = Thread.model_validate(
            {
                "name": "loom_thread",
                "config_version": "1",
                "sources": {"main": {"type": "delta", "alias": src}},
                "target": {"path": tgt},
            }
        )

        weaves = {"lifecycle_weave": weave}
        threads = {"lifecycle_weave": {"loom_thread": thread}}

        with patch("weevr.engine.hooks.logger") as mock_logger:
            result = execute_loom(spark, loom, weaves, threads)

        assert result.status == "success"
        assert result.loom_name == "lifecycle_loom"

        log_messages = [str(call) for call in mock_logger.log.call_args_list]
        assert any("loom-pre-fired" in msg for msg in log_messages)
        assert any("loom-post-fired" in msg for msg in log_messages)
