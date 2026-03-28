"""Tests for ColumnSet, ColumnSetSource, and ReservedWordConfig models."""

import pytest
from pydantic import ValidationError

from weevr.model.column_set import (
    ColumnSet,
    ColumnSetSource,
    ReservedWordConfig,
    ReservedWordPreset,
)


class TestColumnSetSource:
    """Test ColumnSetSource construction and validation."""

    def test_delta_source(self):
        """Delta-sourced column set with all fields."""
        source = ColumnSetSource(
            type="delta",
            alias="silver.dim_customer",
            from_column="source_col",
            to_column="target_col",
            filter="is_active = true",
        )
        assert source.type == "delta"
        assert source.alias == "silver.dim_customer"
        assert source.from_column == "source_col"
        assert source.to_column == "target_col"
        assert source.filter == "is_active = true"
        assert source.path is None

    def test_yaml_source(self):
        """YAML-sourced column set with path."""
        source = ColumnSetSource(type="yaml", path="mappings/customer.yaml")
        assert source.type == "yaml"
        assert source.path == "mappings/customer.yaml"
        assert source.alias is None

    def test_default_from_column(self):
        """from_column defaults to 'source_name'."""
        source = ColumnSetSource(type="delta", alias="ref_t")
        assert source.from_column == "source_name"

    def test_default_to_column(self):
        """to_column defaults to 'target_name'."""
        source = ColumnSetSource(type="delta", alias="ref_t")
        assert source.to_column == "target_name"

    def test_frozen(self):
        """ColumnSetSource is immutable."""
        source = ColumnSetSource(type="delta", alias="ref_t")
        with pytest.raises(ValidationError):
            source.alias = "other"  # type: ignore[misc]


class TestColumnSet:
    """Test ColumnSet construction and validation."""

    def test_delta_sourced(self):
        """Valid column set backed by a Delta source."""
        cs = ColumnSet(
            source=ColumnSetSource(
                type="delta",
                alias="silver.col_map",
                from_column="src",
                to_column="tgt",
                filter="active = true",
            )
        )
        assert cs.source is not None
        assert cs.source.type == "delta"
        assert cs.param is None

    def test_yaml_sourced(self):
        """Valid column set backed by a YAML source."""
        cs = ColumnSet(source=ColumnSetSource(type="yaml", path="maps/cols.yaml"))
        assert cs.source is not None
        assert cs.source.type == "yaml"
        assert cs.param is None

    def test_param_sourced(self):
        """Valid column set backed by a runtime parameter."""
        cs = ColumnSet(param="notebook_param_col_set")
        assert cs.param == "notebook_param_col_set"
        assert cs.source is None

    def test_source_and_param_mutually_exclusive(self):
        """Providing both source and param raises ValidationError."""
        with pytest.raises(ValidationError):
            ColumnSet(
                source=ColumnSetSource(type="delta", alias="ref_t"),
                param="some_param",
            )

    def test_neither_source_nor_param_rejected(self):
        """Providing neither source nor param raises ValidationError."""
        with pytest.raises(ValidationError):
            ColumnSet()

    def test_default_on_unmapped(self):
        """on_unmapped defaults to 'pass_through'."""
        cs = ColumnSet(param="p")
        assert cs.on_unmapped == "pass_through"

    def test_default_on_extra(self):
        """on_extra defaults to 'ignore'."""
        cs = ColumnSet(param="p")
        assert cs.on_extra == "ignore"

    def test_default_on_failure(self):
        """on_failure defaults to 'abort'."""
        cs = ColumnSet(param="p")
        assert cs.on_failure == "abort"

    def test_on_unmapped_error(self):
        """on_unmapped accepts 'error'."""
        cs = ColumnSet(param="p", on_unmapped="error")
        assert cs.on_unmapped == "error"

    def test_on_extra_warn(self):
        """on_extra accepts 'warn'."""
        cs = ColumnSet(param="p", on_extra="warn")
        assert cs.on_extra == "warn"

    def test_on_extra_error(self):
        """on_extra accepts 'error'."""
        cs = ColumnSet(param="p", on_extra="error")
        assert cs.on_extra == "error"

    def test_on_failure_warn(self):
        """on_failure accepts 'warn'."""
        cs = ColumnSet(param="p", on_failure="warn")
        assert cs.on_failure == "warn"

    def test_on_failure_skip(self):
        """on_failure accepts 'skip'."""
        cs = ColumnSet(param="p", on_failure="skip")
        assert cs.on_failure == "skip"

    def test_frozen(self):
        """ColumnSet is immutable."""
        cs = ColumnSet(param="p")
        with pytest.raises(ValidationError):
            cs.param = "other"  # type: ignore[misc]

    def test_from_dict_delta(self):
        """Full delta-backed column set constructed from YAML-like dict."""
        data = {
            "source": {
                "type": "delta",
                "alias": "silver.col_map",
                "from_column": "source_name",
                "to_column": "target_name",
                "filter": "active = true",
            },
            "on_unmapped": "error",
            "on_extra": "warn",
            "on_failure": "skip",
        }
        cs = ColumnSet(**data)
        assert cs.source is not None
        assert cs.source.type == "delta"
        assert cs.source.alias == "silver.col_map"
        assert cs.on_unmapped == "error"
        assert cs.on_extra == "warn"
        assert cs.on_failure == "skip"


class TestWeaveColumnSets:
    """Test column_sets integration on the Weave model."""

    def test_weave_with_column_sets(self):
        """Weave accepts a column_sets dict of ColumnSet instances."""
        from weevr.model.weave import ThreadEntry, Weave

        weave = Weave(
            config_version="1.0",
            threads=[ThreadEntry(name="thread_a")],
            column_sets={
                "sap_dict": ColumnSet(source=ColumnSetSource(type="delta", alias="silver.sap_map"))
            },
        )
        assert weave.column_sets is not None
        assert "sap_dict" in weave.column_sets
        assert weave.column_sets["sap_dict"].source is not None
        assert weave.column_sets["sap_dict"].source.type == "delta"

    def test_weave_column_sets_defaults_none(self):
        """Weave.column_sets defaults to None when not provided."""
        from weevr.model.weave import ThreadEntry, Weave

        weave = Weave(config_version="1.0", threads=[ThreadEntry(name="thread_a")])
        assert weave.column_sets is None

    def test_weave_column_sets_multiple_entries(self):
        """Weave.column_sets supports multiple named column sets."""
        from weevr.model.weave import ThreadEntry, Weave

        weave = Weave(
            config_version="1.0",
            threads=[ThreadEntry(name="thread_a")],
            column_sets={
                "sap_dict": ColumnSet(param="param_sap"),
                "hr_dict": ColumnSet(source=ColumnSetSource(type="yaml", path="maps/hr.yaml")),
            },
        )
        assert weave.column_sets is not None
        assert len(weave.column_sets) == 2
        assert weave.column_sets["sap_dict"].param == "param_sap"
        assert weave.column_sets["hr_dict"].source is not None
        assert weave.column_sets["hr_dict"].source.type == "yaml"


class TestLoomColumnSets:
    """Test column_sets integration on the Loom model."""

    def test_loom_with_column_sets(self):
        """Loom accepts a column_sets dict of ColumnSet instances."""
        from weevr.model.loom import Loom, WeaveEntry

        loom = Loom(
            config_version="1.0",
            weaves=[WeaveEntry(name="weave_a")],
            column_sets={
                "sap_dict": ColumnSet(source=ColumnSetSource(type="delta", alias="gold.sap_map"))
            },
        )
        assert loom.column_sets is not None
        assert "sap_dict" in loom.column_sets
        assert loom.column_sets["sap_dict"].source is not None
        assert loom.column_sets["sap_dict"].source.alias == "gold.sap_map"

    def test_loom_column_sets_defaults_none(self):
        """Loom.column_sets defaults to None when not provided."""
        from weevr.model.loom import Loom, WeaveEntry

        loom = Loom(config_version="1.0", weaves=[WeaveEntry(name="weave_a")])
        assert loom.column_sets is None

    def test_loom_column_sets_multiple_entries(self):
        """Loom.column_sets supports multiple named column sets."""
        from weevr.model.loom import Loom, WeaveEntry

        loom = Loom(
            config_version="1.0",
            weaves=[WeaveEntry(name="weave_a")],
            column_sets={
                "finance_dict": ColumnSet(param="param_finance"),
                "hr_dict": ColumnSet(source=ColumnSetSource(type="yaml", path="maps/hr.yaml")),
            },
        )
        assert loom.column_sets is not None
        assert len(loom.column_sets) == 2
        assert loom.column_sets["finance_dict"].param == "param_finance"


class TestReservedWordConfig:
    """Test ReservedWordConfig construction and validation."""

    def test_defaults(self):
        """All fields default to documented values."""
        cfg = ReservedWordConfig()
        assert cfg.strategy == "quote"
        assert cfg.prefix == "_"
        assert cfg.extend == []
        assert cfg.exclude == []

    def test_prefix_strategy(self):
        """Prefix strategy with custom prefix is accepted."""
        cfg = ReservedWordConfig(strategy="prefix", prefix="col_")
        assert cfg.strategy == "prefix"
        assert cfg.prefix == "col_"

    def test_error_strategy(self):
        """Error strategy is accepted."""
        cfg = ReservedWordConfig(strategy="error")
        assert cfg.strategy == "error"

    def test_extend_and_exclude(self):
        """Custom extend and exclude lists are stored correctly."""
        cfg = ReservedWordConfig(extend=["TIMESTAMP", "VALUE"], exclude=["id"])
        assert cfg.extend == ["TIMESTAMP", "VALUE"]
        assert cfg.exclude == ["id"]

    def test_preset_defaults_to_none(self):
        """preset defaults to None when not specified."""
        cfg = ReservedWordConfig()
        assert cfg.preset is None

    def test_preset_accepts_list(self):
        """preset accepts a list of valid preset names."""
        cfg = ReservedWordConfig(preset=["ansi", "dax"])  # type: ignore[arg-type]
        assert cfg.preset == [ReservedWordPreset.ANSI, ReservedWordPreset.DAX]

    def test_preset_string_sugar(self):
        """Single string is normalised to a one-element list."""
        cfg = ReservedWordConfig(preset="dax")  # type: ignore[arg-type]
        assert cfg.preset == [ReservedWordPreset.DAX]

    def test_preset_unknown_raises(self):
        """Unknown preset name raises ValidationError."""
        with pytest.raises(ValidationError):
            ReservedWordConfig(preset=["nonexistent"])  # type: ignore[arg-type]

    def test_preset_empty_list(self):
        """Empty preset list is accepted."""
        cfg = ReservedWordConfig(preset=[])
        assert cfg.preset == []

    def test_preset_all_values(self):
        """All five preset values are accepted."""
        cfg = ReservedWordConfig(preset=["ansi", "dax", "m", "powerbi", "tsql"])  # type: ignore[arg-type]
        assert cfg.preset is not None and len(cfg.preset) == 5

    def test_frozen(self):
        """ReservedWordConfig is immutable."""
        cfg = ReservedWordConfig()
        with pytest.raises(ValidationError):
            cfg.strategy = "prefix"  # type: ignore[misc]


class TestReservedWordPreset:
    """Test ReservedWordPreset enum."""

    def test_has_five_members(self):
        """Enum has exactly 5 members."""
        assert len(ReservedWordPreset) == 5

    def test_values_are_lowercase(self):
        """All enum values are lowercase strings."""
        for preset in ReservedWordPreset:
            assert preset.value == preset.value.lower()

    def test_string_comparison(self):
        """StrEnum members compare equal to their string values."""
        assert ReservedWordPreset.ANSI == "ansi"
        assert ReservedWordPreset.DAX == "dax"
        assert ReservedWordPreset.M == "m"
        assert ReservedWordPreset.POWERBI == "powerbi"
        assert ReservedWordPreset.TSQL == "tsql"
