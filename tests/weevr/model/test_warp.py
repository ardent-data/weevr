"""Tests for WarpColumn, WarpKeys, and WarpConfig models."""

import pytest
from pydantic import ValidationError

from weevr.model.warp import WarpColumn, WarpConfig, WarpKeys


class TestWarpColumn:
    """Test WarpColumn model."""

    def test_minimal_requires_name_and_type(self):
        """WarpColumn requires name and type."""
        col = WarpColumn(name="customer_id", type="bigint")
        assert col.name == "customer_id"
        assert col.type == "bigint"

    def test_defaults(self):
        """WarpColumn has sensible defaults."""
        col = WarpColumn(name="customer_id", type="bigint")
        assert col.nullable is True
        assert col.default is None
        assert col.description is None
        assert col.discovered is False

    def test_nullable_false(self):
        """WarpColumn accepts nullable=False."""
        col = WarpColumn(name="id", type="bigint", nullable=False)
        assert col.nullable is False

    def test_with_default(self):
        """WarpColumn accepts a default value."""
        col = WarpColumn(name="status", type="string", default="active")
        assert col.default == "active"

    def test_with_description(self):
        """WarpColumn accepts a description."""
        col = WarpColumn(name="id", type="bigint", description="Primary key")
        assert col.description == "Primary key"

    def test_discovered_flag(self):
        """WarpColumn accepts discovered=True."""
        col = WarpColumn(name="new_col", type="string", discovered=True)
        assert col.discovered is True

    def test_frozen(self):
        """WarpColumn is immutable."""
        col = WarpColumn(name="id", type="bigint")
        with pytest.raises(ValidationError):
            col.name = "other"  # type: ignore[misc]

    def test_round_trip(self):
        """WarpColumn round-trips through serialization."""
        col = WarpColumn(
            name="amount",
            type="decimal(18,2)",
            nullable=False,
            default=0,
            description="Transaction amount",
            discovered=True,
        )
        assert WarpColumn.model_validate(col.model_dump()) == col


class TestWarpKeys:
    """Test WarpKeys model."""

    def test_both_optional(self):
        """WarpKeys with no fields is valid."""
        keys = WarpKeys()
        assert keys.surrogate is None
        assert keys.business is None

    def test_with_surrogate(self):
        """WarpKeys accepts a surrogate key name."""
        keys = WarpKeys(surrogate="customer_sk")
        assert keys.surrogate == "customer_sk"

    def test_with_business(self):
        """WarpKeys accepts business key columns."""
        keys = WarpKeys(business=["customer_id", "source_system"])
        assert keys.business == ["customer_id", "source_system"]

    def test_with_both(self):
        """WarpKeys accepts both surrogate and business."""
        keys = WarpKeys(surrogate="customer_sk", business=["customer_id"])
        assert keys.surrogate == "customer_sk"
        assert keys.business == ["customer_id"]

    def test_frozen(self):
        """WarpKeys is immutable."""
        keys = WarpKeys(surrogate="sk")
        with pytest.raises(ValidationError):
            keys.surrogate = "other"  # type: ignore[misc]


class TestWarpConfig:
    """Test WarpConfig model."""

    def test_minimal_valid(self):
        """WarpConfig with config_version and columns is valid."""
        warp = WarpConfig(
            config_version="1.0",
            columns=[  # type: ignore[arg-type]
                {"name": "customer_id", "type": "bigint"},
                {"name": "customer_name", "type": "string"},
            ],
        )
        assert warp.config_version == "1.0"
        assert len(warp.columns) == 2

    def test_config_version_required(self):
        """WarpConfig without config_version raises ValidationError."""
        with pytest.raises(ValidationError):
            WarpConfig(
                columns=[{"name": "id", "type": "bigint"}],  # type: ignore[arg-type]
            )

    def test_columns_required(self):
        """WarpConfig without columns raises ValidationError."""
        with pytest.raises(ValidationError):
            WarpConfig(config_version="1.0")  # type: ignore[call-arg]

    def test_with_keys(self):
        """WarpConfig accepts a keys section."""
        warp = WarpConfig(
            config_version="1.0",
            columns=[  # type: ignore[arg-type]
                {"name": "customer_sk", "type": "bigint"},
                {"name": "customer_id", "type": "string"},
            ],
            keys={"surrogate": "customer_sk", "business": ["customer_id"]},  # type: ignore[arg-type]
        )
        assert warp.keys is not None
        assert warp.keys.surrogate == "customer_sk"
        assert warp.keys.business == ["customer_id"]

    def test_auto_generated_flag(self):
        """WarpConfig accepts auto_generated marker."""
        warp = WarpConfig(
            config_version="1.0",
            columns=[{"name": "id", "type": "bigint"}],  # type: ignore[arg-type]
            auto_generated=True,
        )
        assert warp.auto_generated is True

    def test_auto_generated_default_false(self):
        """auto_generated defaults to False."""
        warp = WarpConfig(
            config_version="1.0",
            columns=[{"name": "id", "type": "bigint"}],  # type: ignore[arg-type]
        )
        assert warp.auto_generated is False

    def test_duplicate_column_names_raises(self):
        """WarpConfig rejects duplicate column names."""
        with pytest.raises(ValidationError, match="[Dd]uplicate"):
            WarpConfig(
                config_version="1.0",
                columns=[  # type: ignore[arg-type]
                    {"name": "id", "type": "bigint"},
                    {"name": "id", "type": "string"},
                ],
            )

    def test_keys_surrogate_references_valid_column(self):
        """keys.surrogate must reference a declared column."""
        with pytest.raises(ValidationError, match="surrogate"):
            WarpConfig(
                config_version="1.0",
                columns=[{"name": "id", "type": "bigint"}],  # type: ignore[arg-type]
                keys={"surrogate": "nonexistent_sk"},  # type: ignore[arg-type]
            )

    def test_keys_business_references_valid_columns(self):
        """keys.business entries must reference declared columns."""
        with pytest.raises(ValidationError, match="business"):
            WarpConfig(
                config_version="1.0",
                columns=[{"name": "id", "type": "bigint"}],  # type: ignore[arg-type]
                keys={"business": ["id", "missing_col"]},  # type: ignore[arg-type]
            )

    def test_keys_surrogate_valid_reference(self):
        """keys.surrogate referencing a valid column passes."""
        warp = WarpConfig(
            config_version="1.0",
            columns=[  # type: ignore[arg-type]
                {"name": "customer_sk", "type": "bigint"},
                {"name": "customer_id", "type": "string"},
            ],
            keys={"surrogate": "customer_sk"},  # type: ignore[arg-type]
        )
        assert warp.keys is not None
        assert warp.keys.surrogate == "customer_sk"

    def test_keys_business_valid_references(self):
        """keys.business referencing valid columns passes."""
        warp = WarpConfig(
            config_version="1.0",
            columns=[  # type: ignore[arg-type]
                {"name": "customer_id", "type": "string"},
                {"name": "source_system", "type": "string"},
            ],
            keys={"business": ["customer_id", "source_system"]},  # type: ignore[arg-type]
        )
        assert warp.keys is not None
        assert warp.keys.business == ["customer_id", "source_system"]

    def test_frozen(self):
        """WarpConfig is immutable."""
        warp = WarpConfig(
            config_version="1.0",
            columns=[{"name": "id", "type": "bigint"}],  # type: ignore[arg-type]
        )
        with pytest.raises(ValidationError):
            warp.config_version = "2.0"  # type: ignore[misc]

    def test_round_trip(self):
        """WarpConfig round-trips through serialization."""
        warp = WarpConfig(
            config_version="1.0",
            columns=[  # type: ignore[arg-type]
                {"name": "customer_sk", "type": "bigint", "nullable": False},
                {
                    "name": "customer_name",
                    "type": "string",
                    "description": "Full name",
                },
            ],
            keys={"surrogate": "customer_sk", "business": ["customer_name"]},  # type: ignore[arg-type]
            auto_generated=True,
        )
        assert WarpConfig.model_validate(warp.model_dump()) == warp

    def test_description_field(self):
        """WarpConfig accepts a top-level description."""
        warp = WarpConfig(
            config_version="1.0",
            columns=[{"name": "id", "type": "bigint"}],  # type: ignore[arg-type]
            description="Customer dimension schema contract",
        )
        assert warp.description == "Customer dimension schema contract"

    def test_empty_columns_raises(self):
        """WarpConfig rejects empty columns list."""
        with pytest.raises(ValidationError):
            WarpConfig(config_version="1.0", columns=[])  # type: ignore[arg-type]
