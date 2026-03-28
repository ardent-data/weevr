"""Tests for DimensionConfig and related nested models."""

import pytest
from pydantic import ValidationError

from weevr.model.dimension import (
    AdditionalKeyConfig,
    ChangeDetectionGroupConfig,
    DimensionConfig,
    ScdColumnConfig,
    ScdDateConfig,
    SurrogateKeyConfig,
    SystemMemberConfig,
)


class TestSurrogateKeyConfig:
    """Test DimensionConfig's SurrogateKeyConfig (includes columns field)."""

    def test_minimal(self):
        """SurrogateKeyConfig with name and columns."""
        s = SurrogateKeyConfig(name="sk_id", columns=["customer_id"])
        assert s.name == "sk_id"
        assert s.columns == ["customer_id"]
        assert s.algorithm == "sha256"
        assert s.output == "native"

    def test_full(self):
        """SurrogateKeyConfig with all fields."""
        s = SurrogateKeyConfig(
            name="sk_id",
            algorithm="xxhash64",
            columns=["customer_id", "region"],
            output="string",
        )
        assert s.algorithm == "xxhash64"
        assert s.columns == ["customer_id", "region"]
        assert s.output == "string"

    def test_invalid_algorithm_raises(self):
        """Unknown algorithm raises ValidationError."""
        with pytest.raises(ValidationError):
            SurrogateKeyConfig(
                name="sk_id",
                columns=["id"],
                algorithm="blake2",  # type: ignore[arg-type]
            )

    def test_frozen(self):
        """SurrogateKeyConfig is immutable."""
        s = SurrogateKeyConfig(name="sk_id", columns=["id"])
        with pytest.raises(ValidationError):
            s.name = "other"  # type: ignore[misc]

    def test_round_trip(self):
        """SurrogateKeyConfig round-trips through model_dump."""
        s = SurrogateKeyConfig(name="sk_id", columns=["id"], algorithm="md5")
        assert SurrogateKeyConfig.model_validate(s.model_dump()) == s


class TestChangeDetectionGroupConfig:
    """Test ChangeDetectionGroupConfig model."""

    def test_minimal(self):
        """ChangeDetectionGroupConfig with required fields."""
        g = ChangeDetectionGroupConfig(
            columns=["col_a", "col_b"],
            on_change="overwrite",
        )
        assert g.columns == ["col_a", "col_b"]
        assert g.on_change == "overwrite"
        assert g.name is None
        assert g.algorithm is None
        assert g.output == "native"

    def test_full(self):
        """ChangeDetectionGroupConfig with all fields."""
        g = ChangeDetectionGroupConfig(
            name="attrs",
            algorithm="sha256",
            columns=["col_a", "col_b"],
            on_change="version",
            output="string",
        )
        assert g.name == "attrs"
        assert g.algorithm == "sha256"
        assert g.on_change == "version"
        assert g.output == "string"

    def test_auto_columns(self):
        """ChangeDetectionGroupConfig accepts 'auto' for columns."""
        g = ChangeDetectionGroupConfig(columns="auto", on_change="overwrite")
        assert g.columns == "auto"

    def test_static_on_change(self):
        """ChangeDetectionGroupConfig accepts 'static' on_change."""
        g = ChangeDetectionGroupConfig(columns=["col_a"], on_change="static")
        assert g.on_change == "static"

    def test_invalid_on_change_raises(self):
        """Invalid on_change raises ValidationError."""
        with pytest.raises(ValidationError):
            ChangeDetectionGroupConfig(
                columns=["col_a"],
                on_change="archive",  # type: ignore[arg-type]
            )

    def test_frozen(self):
        """ChangeDetectionGroupConfig is immutable."""
        g = ChangeDetectionGroupConfig(columns=["col_a"], on_change="overwrite")
        with pytest.raises(ValidationError):
            g.on_change = "version"  # type: ignore[misc]

    def test_round_trip(self):
        """ChangeDetectionGroupConfig round-trips."""
        g = ChangeDetectionGroupConfig(
            name="grp",
            algorithm="sha256",
            columns=["col_a"],
            on_change="version",
        )
        assert ChangeDetectionGroupConfig.model_validate(g.model_dump()) == g


class TestAdditionalKeyConfig:
    """Test AdditionalKeyConfig model."""

    def test_minimal(self):
        """AdditionalKeyConfig with name and columns."""
        k = AdditionalKeyConfig(name="ak_region", columns=["region", "country"])
        assert k.name == "ak_region"
        assert k.columns == ["region", "country"]
        assert k.algorithm == "sha256"
        assert k.output == "native"

    def test_custom_algorithm(self):
        """AdditionalKeyConfig with custom algorithm."""
        k = AdditionalKeyConfig(name="ak", columns=["id"], algorithm="md5")
        assert k.algorithm == "md5"

    def test_invalid_algorithm_raises(self):
        """Invalid algorithm raises ValidationError."""
        with pytest.raises(ValidationError):
            AdditionalKeyConfig(
                name="ak",
                columns=["id"],
                algorithm="blake2",  # type: ignore[arg-type]
            )

    def test_frozen(self):
        """AdditionalKeyConfig is immutable."""
        k = AdditionalKeyConfig(name="ak", columns=["id"])
        with pytest.raises(ValidationError):
            k.name = "other"  # type: ignore[misc]


class TestSystemMemberConfig:
    """Test SystemMemberConfig model."""

    def test_valid_negative_sk(self):
        """SystemMemberConfig with negative SK value."""
        m = SystemMemberConfig(sk=-1, code="UNKNOWN", label="Unknown")
        assert m.sk == -1
        assert m.code == "UNKNOWN"
        assert m.label == "Unknown"

    def test_zero_sk_raises(self):
        """SK of zero raises ValidationError."""
        with pytest.raises(ValidationError):
            SystemMemberConfig(sk=0, code="UNKNOWN", label="Unknown")

    def test_positive_sk_raises(self):
        """Positive SK raises ValidationError."""
        with pytest.raises(ValidationError):
            SystemMemberConfig(sk=1, code="UNKNOWN", label="Unknown")

    def test_frozen(self):
        """SystemMemberConfig is immutable."""
        m = SystemMemberConfig(sk=-1, code="UNKNOWN", label="Unknown")
        with pytest.raises(ValidationError):
            m.sk = -2  # type: ignore[misc]

    def test_round_trip(self):
        """SystemMemberConfig round-trips."""
        m = SystemMemberConfig(sk=-99, code="NA", label="Not Applicable")
        assert SystemMemberConfig.model_validate(m.model_dump()) == m


class TestScdColumnConfig:
    """Test ScdColumnConfig model."""

    def test_defaults(self):
        """ScdColumnConfig applies default column names."""
        c = ScdColumnConfig()
        assert c.valid_from == "_valid_from"
        assert c.valid_to == "_valid_to"
        assert c.is_current == "_is_current"

    def test_custom_names(self):
        """ScdColumnConfig accepts custom column names."""
        c = ScdColumnConfig(
            valid_from="eff_from",
            valid_to="eff_to",
            is_current="is_active",
        )
        assert c.valid_from == "eff_from"
        assert c.valid_to == "eff_to"
        assert c.is_current == "is_active"

    def test_frozen(self):
        """ScdColumnConfig is immutable."""
        c = ScdColumnConfig()
        with pytest.raises(ValidationError):
            c.valid_from = "other"  # type: ignore[misc]


class TestScdDateConfig:
    """Test ScdDateConfig model."""

    def test_defaults(self):
        """ScdDateConfig applies default min and max dates."""
        d = ScdDateConfig()
        assert d.min == "1970-01-01"
        assert d.max == "9999-12-31"

    def test_custom_dates(self):
        """ScdDateConfig accepts custom date boundaries."""
        d = ScdDateConfig(min="2000-01-01", max="2099-12-31")
        assert d.min == "2000-01-01"
        assert d.max == "2099-12-31"

    def test_min_equal_to_max_raises(self):
        """min equal to max raises ValidationError."""
        with pytest.raises(ValidationError):
            ScdDateConfig(min="2000-01-01", max="2000-01-01")

    def test_min_after_max_raises(self):
        """min after max raises ValidationError."""
        with pytest.raises(ValidationError):
            ScdDateConfig(min="2100-01-01", max="2000-01-01")

    def test_frozen(self):
        """ScdDateConfig is immutable."""
        d = ScdDateConfig()
        with pytest.raises(ValidationError):
            d.min = "2000-01-01"  # type: ignore[misc]


class TestDimensionConfigMinimal:
    """Test DimensionConfig with minimal configuration."""

    def test_minimal_valid(self):
        """DimensionConfig with only required fields."""
        d = DimensionConfig(
            business_key=["customer_id"],
            surrogate_key={"name": "sk_customer", "columns": ["customer_id"]},  # type: ignore[arg-type]
        )
        assert d.business_key == ["customer_id"]
        assert d.surrogate_key.name == "sk_customer"
        assert d.track_history is False
        assert d.change_detection is not None
        assert d.additional_keys is None
        assert d.previous_columns is None
        assert d.seed_system_members is False
        assert d.system_members is None
        assert d.label_column is None
        assert d.history_filter is True

    def test_default_scd_columns_applied(self):
        """ScdColumnConfig defaults are applied when columns section omitted."""
        d = DimensionConfig(
            business_key=["id"],
            surrogate_key={"name": "sk_id", "columns": ["id"]},  # type: ignore[arg-type]
        )
        assert d.columns.valid_from == "_valid_from"
        assert d.columns.valid_to == "_valid_to"
        assert d.columns.is_current == "_is_current"

    def test_default_dates_applied(self):
        """ScdDateConfig defaults are applied when dates section omitted."""
        d = DimensionConfig(
            business_key=["id"],
            surrogate_key={"name": "sk_id", "columns": ["id"]},  # type: ignore[arg-type]
        )
        assert d.dates.min == "1970-01-01"
        assert d.dates.max == "9999-12-31"

    def test_empty_business_key_raises(self):
        """Empty business_key list raises ValidationError."""
        with pytest.raises(ValidationError):
            DimensionConfig(
                business_key=[],
                surrogate_key={"name": "sk_id", "columns": ["id"]},  # type: ignore[arg-type]
            )

    def test_empty_surrogate_key_columns_raises(self):
        """Empty surrogate_key.columns raises ValidationError."""
        with pytest.raises(ValidationError):
            DimensionConfig(
                business_key=["id"],
                surrogate_key={"name": "sk_id", "columns": []},  # type: ignore[arg-type]
            )

    def test_frozen(self):
        """DimensionConfig is immutable."""
        d = DimensionConfig(
            business_key=["id"],
            surrogate_key={"name": "sk_id", "columns": ["id"]},  # type: ignore[arg-type]
        )
        with pytest.raises(ValidationError):
            d.track_history = True  # type: ignore[misc]


class TestDimensionConfigFull:
    """Test DimensionConfig with all optional fields populated."""

    def test_full_config(self):
        """DimensionConfig with all fields populated."""
        d = DimensionConfig(
            business_key=["customer_id"],
            surrogate_key={"name": "sk_customer", "columns": ["customer_id"]},  # type: ignore[arg-type]
            track_history=True,
            previous_columns={"_prev_name": "customer_name"},
            change_detection={  # type: ignore[arg-type]
                "attrs": {
                    "name": "attrs",
                    "algorithm": "sha256",
                    "columns": ["name", "email"],
                    "on_change": "version",
                },
                "meta": {
                    "name": "meta",
                    "columns": ["region"],
                    "on_change": "overwrite",
                },
            },
            additional_keys={  # type: ignore[arg-type]
                "ak_region": {
                    "name": "ak_region",
                    "columns": ["region"],
                    "algorithm": "md5",
                },
            },
            columns={"valid_from": "eff_from", "valid_to": "eff_to", "is_current": "is_active"},  # type: ignore[arg-type]
            dates={"min": "2000-01-01", "max": "2099-12-31"},  # type: ignore[arg-type]
            seed_system_members=True,
            system_members=[  # type: ignore[arg-type]
                {"sk": -1, "code": "UNKNOWN", "label": "Unknown"},
                {"sk": -2, "code": "NA", "label": "Not Applicable"},
            ],
            label_column="_label",
            history_filter=False,
        )
        assert d.business_key == ["customer_id"]
        assert d.track_history is True
        assert d.seed_system_members is True
        assert len(d.system_members) == 2  # type: ignore[arg-type]
        assert d.label_column == "_label"
        assert d.history_filter is False
        assert d.previous_columns == {"_prev_name": "customer_name"}

    def test_round_trip(self):
        """DimensionConfig round-trips through model_dump."""
        d = DimensionConfig(
            business_key=["id"],
            surrogate_key={"name": "sk_id", "columns": ["id"]},  # type: ignore[arg-type]
            track_history=True,
            change_detection={  # type: ignore[arg-type]
                "grp": {
                    "name": "grp",
                    "columns": ["col_a"],
                    "on_change": "version",
                },
            },
        )
        assert DimensionConfig.model_validate(d.model_dump()) == d


class TestDimensionConfigTrackHistory:
    """Test track_history and on_change interaction rules."""

    def test_track_history_false_version_on_change_raises(self):
        """on_change: version is invalid when track_history is False."""
        with pytest.raises(ValidationError):
            DimensionConfig(
                business_key=["id"],
                surrogate_key={"name": "sk_id", "columns": ["id"]},  # type: ignore[arg-type]
                track_history=False,
                change_detection={  # type: ignore[arg-type]
                    "grp": {
                        "columns": ["col_a"],
                        "on_change": "version",
                    },
                },
            )

    def test_track_history_true_version_on_change_valid(self):
        """on_change: version is valid when track_history is True."""
        d = DimensionConfig(
            business_key=["id"],
            surrogate_key={"name": "sk_id", "columns": ["id"]},  # type: ignore[arg-type]
            track_history=True,
            change_detection={  # type: ignore[arg-type]
                "grp": {
                    "columns": ["col_a"],
                    "on_change": "version",
                },
            },
        )
        assert d.track_history is True

    def test_track_history_false_overwrite_valid(self):
        """on_change: overwrite is valid regardless of track_history."""
        d = DimensionConfig(
            business_key=["id"],
            surrogate_key={"name": "sk_id", "columns": ["id"]},  # type: ignore[arg-type]
            track_history=False,
            change_detection={  # type: ignore[arg-type]
                "grp": {
                    "columns": ["col_a"],
                    "on_change": "overwrite",
                },
            },
        )
        assert d.track_history is False


class TestDimensionConfigNamedGroups:
    """Test named change_detection group validation rules."""

    def test_column_in_multiple_groups_raises(self):
        """A column appearing in multiple groups raises ValidationError."""
        with pytest.raises(ValidationError):
            DimensionConfig(
                business_key=["id"],
                surrogate_key={"name": "sk_id", "columns": ["id"]},  # type: ignore[arg-type]
                change_detection={  # type: ignore[arg-type]
                    "grp1": {
                        "columns": ["col_a", "col_b"],
                        "on_change": "overwrite",
                    },
                    "grp2": {
                        "columns": ["col_b", "col_c"],
                        "on_change": "overwrite",
                    },
                },
            )

    def test_multiple_auto_groups_raises(self):
        """More than one auto group raises ValidationError."""
        with pytest.raises(ValidationError):
            DimensionConfig(
                business_key=["id"],
                surrogate_key={"name": "sk_id", "columns": ["id"]},  # type: ignore[arg-type]
                change_detection={  # type: ignore[arg-type]
                    "grp1": {
                        "columns": "auto",
                        "on_change": "overwrite",
                    },
                    "grp2": {
                        "columns": "auto",
                        "on_change": "overwrite",
                    },
                },
            )

    def test_single_auto_group_valid(self):
        """A single auto group is valid."""
        d = DimensionConfig(
            business_key=["id"],
            surrogate_key={"name": "sk_id", "columns": ["id"]},  # type: ignore[arg-type]
            change_detection={  # type: ignore[arg-type]
                "grp": {
                    "columns": "auto",
                    "on_change": "overwrite",
                },
            },
        )
        assert d.change_detection is not None
        assert d.change_detection["grp"].columns == "auto"

    def test_disjoint_column_groups_valid(self):
        """Groups with non-overlapping columns are valid."""
        d = DimensionConfig(
            business_key=["id"],
            surrogate_key={"name": "sk_id", "columns": ["id"]},  # type: ignore[arg-type]
            change_detection={  # type: ignore[arg-type]
                "grp1": {
                    "columns": ["col_a"],
                    "on_change": "overwrite",
                },
                "grp2": {
                    "columns": ["col_b"],
                    "on_change": "overwrite",
                },
            },
        )
        assert len(d.change_detection) == 2  # type: ignore[arg-type]


class TestDimensionConfigDefaultChangeDetection:
    """Test that change_detection is auto-populated when omitted."""

    def test_default_change_detection_populated_when_omitted(self):
        """When change_detection is omitted, a default group is created."""
        d = DimensionConfig(
            business_key=["id"],
            surrogate_key={"name": "sk_id", "columns": ["id"]},  # type: ignore[arg-type]
        )
        assert d.change_detection is not None

    def test_default_change_detection_has_single_group(self):
        """Default change_detection contains exactly one group keyed '_default'."""
        d = DimensionConfig(
            business_key=["id"],
            surrogate_key={"name": "sk_id", "columns": ["id"]},  # type: ignore[arg-type]
        )
        assert d.change_detection is not None
        assert "_default" in d.change_detection
        assert len(d.change_detection) == 1

    def test_default_group_name_is_row_hash(self):
        """Default group has name='_row_hash'."""
        d = DimensionConfig(
            business_key=["id"],
            surrogate_key={"name": "sk_id", "columns": ["id"]},  # type: ignore[arg-type]
        )
        assert d.change_detection is not None
        grp = d.change_detection["_default"]
        assert grp.name == "_row_hash"

    def test_default_group_algorithm_is_sha256(self):
        """Default group has algorithm='sha256'."""
        d = DimensionConfig(
            business_key=["id"],
            surrogate_key={"name": "sk_id", "columns": ["id"]},  # type: ignore[arg-type]
        )
        assert d.change_detection is not None
        grp = d.change_detection["_default"]
        assert grp.algorithm == "sha256"

    def test_default_group_columns_is_auto(self):
        """Default group has columns='auto'."""
        d = DimensionConfig(
            business_key=["id"],
            surrogate_key={"name": "sk_id", "columns": ["id"]},  # type: ignore[arg-type]
        )
        assert d.change_detection is not None
        grp = d.change_detection["_default"]
        assert grp.columns == "auto"

    def test_default_on_change_is_version_when_track_history_true(self):
        """When track_history=True and change_detection omitted, on_change is 'version'."""
        d = DimensionConfig(
            business_key=["id"],
            surrogate_key={"name": "sk_id", "columns": ["id"]},  # type: ignore[arg-type]
            track_history=True,
        )
        assert d.change_detection is not None
        grp = d.change_detection["_default"]
        assert grp.on_change == "version"

    def test_default_on_change_is_overwrite_when_track_history_false(self):
        """When track_history=False and change_detection omitted, on_change is 'overwrite'."""
        d = DimensionConfig(
            business_key=["id"],
            surrogate_key={"name": "sk_id", "columns": ["id"]},  # type: ignore[arg-type]
            track_history=False,
        )
        assert d.change_detection is not None
        grp = d.change_detection["_default"]
        assert grp.on_change == "overwrite"

    def test_explicit_change_detection_not_overridden(self):
        """When change_detection is explicitly provided, it is not replaced by the default."""
        d = DimensionConfig(
            business_key=["id"],
            surrogate_key={"name": "sk_id", "columns": ["id"]},  # type: ignore[arg-type]
            change_detection={  # type: ignore[arg-type]
                "custom": {
                    "columns": ["col_a"],
                    "on_change": "overwrite",
                },
            },
        )
        assert d.change_detection is not None
        assert "custom" in d.change_detection
        assert "_default" not in d.change_detection

    def test_explicit_null_change_detection_triggers_default(self):
        """Explicitly passing change_detection=None still triggers the default."""
        d = DimensionConfig(
            business_key=["id"],
            surrogate_key={"name": "sk_id", "columns": ["id"]},  # type: ignore[arg-type]
            change_detection=None,
        )
        assert d.change_detection is not None
        assert "_default" in d.change_detection


class TestDimensionConfigSystemMembers:
    """Test system member validation."""

    def test_system_members_with_negative_sk_valid(self):
        """System members with all negative SK values are valid."""
        d = DimensionConfig(
            business_key=["id"],
            surrogate_key={"name": "sk_id", "columns": ["id"]},  # type: ignore[arg-type]
            system_members=[  # type: ignore[arg-type]
                {"sk": -1, "code": "UNKNOWN", "label": "Unknown"},
                {"sk": -2, "code": "NA", "label": "Not Applicable"},
            ],
        )
        assert d.system_members is not None
        assert d.system_members[0].sk == -1

    def test_system_member_positive_sk_raises(self):
        """System member with positive SK raises ValidationError."""
        with pytest.raises(ValidationError):
            DimensionConfig(
                business_key=["id"],
                surrogate_key={"name": "sk_id", "columns": ["id"]},  # type: ignore[arg-type]
                system_members=[  # type: ignore[arg-type]
                    {"sk": 1, "code": "BAD", "label": "Bad"},
                ],
            )
