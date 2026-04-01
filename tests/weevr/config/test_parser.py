"""Tests for YAML parser."""

from pathlib import Path

import pytest

from weevr.config.parser import (
    detect_config_type,
    detect_config_type_from_extension,
    extract_config_version,
    parse_yaml,
    validate_config_version,
)
from weevr.errors import ConfigError, ConfigParseError, ConfigVersionError

# Test fixture directory
FIXTURES = Path(__file__).parent / "fixtures"


class TestParseYAML:
    """Test parse_yaml function."""

    def test_valid_yaml(self):
        """Parse valid YAML file."""
        result = parse_yaml(FIXTURES / "valid_thread.thread")
        assert isinstance(result, dict)
        assert "config_version" in result
        assert "sources" in result
        assert "target" in result

    def test_file_not_found(self):
        """Raise ConfigParseError for missing file."""
        with pytest.raises(ConfigParseError) as exc_info:
            parse_yaml(FIXTURES / "nonexistent.yaml")
        assert "not found" in str(exc_info.value)

    def test_invalid_yaml_syntax(self):
        """Raise ConfigParseError for invalid YAML."""
        with pytest.raises(ConfigParseError) as exc_info:
            parse_yaml(FIXTURES / "invalid_yaml.yaml")
        assert "Invalid YAML syntax" in str(exc_info.value)

    def test_empty_file(self, tmp_path):
        """Raise ConfigParseError for empty file."""
        empty_file = tmp_path / "empty.yaml"
        empty_file.write_text("")
        with pytest.raises(ConfigParseError) as exc_info:
            parse_yaml(empty_file)
        assert "empty" in str(exc_info.value).lower()

    def test_non_dict_content(self, tmp_path):
        """Raise ConfigParseError for non-dict YAML."""
        list_file = tmp_path / "list.yaml"
        list_file.write_text("- item1\n- item2\n")
        with pytest.raises(ConfigParseError) as exc_info:
            parse_yaml(list_file)
        assert "Expected YAML dict" in str(exc_info.value)


class TestExtractConfigVersion:
    """Test extract_config_version function."""

    def test_valid_version_1_0(self):
        """Extract version 1.0."""
        raw = {"config_version": "1.0"}
        result = extract_config_version(raw)
        assert result == (1, 0)

    def test_valid_version_1_5(self):
        """Extract version 1.5."""
        raw = {"config_version": "1.5"}
        result = extract_config_version(raw)
        assert result == (1, 5)

    def test_valid_version_2_10(self):
        """Extract version 2.10."""
        raw = {"config_version": "2.10"}
        result = extract_config_version(raw)
        assert result == (2, 10)

    def test_missing_field(self):
        """Raise ConfigParseError for missing config_version."""
        raw = {"sources": {}}
        with pytest.raises(ConfigParseError) as exc_info:
            extract_config_version(raw)
        assert "Missing required field 'config_version'" in str(exc_info.value)

    def test_invalid_format_no_dot(self):
        """Raise ConfigParseError for invalid format (no dot)."""
        raw = {"config_version": "10"}
        with pytest.raises(ConfigParseError) as exc_info:
            extract_config_version(raw)
        assert "major.minor" in str(exc_info.value)

    def test_invalid_format_non_integer(self):
        """Raise ConfigParseError for non-integer parts."""
        raw = {"config_version": "abc.def"}
        with pytest.raises(ConfigParseError) as exc_info:
            extract_config_version(raw)
        assert "integers" in str(exc_info.value)

    def test_non_string_version(self):
        """Raise ConfigParseError for non-string version."""
        raw = {"config_version": 1.0}
        with pytest.raises(ConfigParseError) as exc_info:
            extract_config_version(raw)
        assert "must be a string" in str(exc_info.value)


class TestValidateConfigVersion:
    """Test validate_config_version function."""

    def test_thread_version_1_0(self):
        """Accept version 1.0 for thread."""
        validate_config_version((1, 0), "thread")  # Should not raise

    def test_thread_version_1_5(self):
        """Accept version 1.5 for thread (same major)."""
        validate_config_version((1, 5), "thread")  # Should not raise

    def test_weave_version_1_0(self):
        """Accept version 1.0 for weave."""
        validate_config_version((1, 0), "weave")  # Should not raise

    def test_loom_version_1_0(self):
        """Accept version 1.0 for loom."""
        validate_config_version((1, 0), "loom")  # Should not raise

    def test_thread_unsupported_major(self):
        """Raise ConfigVersionError for unsupported major version."""
        with pytest.raises(ConfigVersionError) as exc_info:
            validate_config_version((2, 0), "thread")
        assert "Unsupported" in str(exc_info.value)
        assert "2.0" in str(exc_info.value)

    def test_warp_version_1_0(self):
        """Accept version 1.0 for warp."""
        validate_config_version((1, 0), "warp")  # Should not raise

    def test_unknown_config_type(self):
        """Raise ConfigVersionError for unknown config type."""
        with pytest.raises(ConfigVersionError) as exc_info:
            validate_config_version((1, 0), "unknown")
        assert "Unknown config type" in str(exc_info.value)


class TestDetectConfigType:
    """Test detect_config_type function."""

    def test_detect_thread_by_sources(self):
        """Detect thread config by 'sources' field."""
        raw = {"config_version": "1.0", "sources": {}, "target": {}}
        assert detect_config_type(raw) == "thread"

    def test_detect_thread_by_target(self):
        """Detect thread config by 'target' field."""
        raw = {"config_version": "1.0", "target": {}}
        assert detect_config_type(raw) == "thread"

    def test_detect_weave(self):
        """Detect weave config by 'threads' field."""
        raw = {"config_version": "1.0", "threads": []}
        assert detect_config_type(raw) == "weave"

    def test_detect_loom(self):
        """Detect loom config by 'weaves' field."""
        raw = {"config_version": "1.0", "weaves": []}
        assert detect_config_type(raw) == "loom"

    def test_detect_params(self):
        """Detect params file by absence of structural keys."""
        raw = {"config_version": "1.0", "env": "dev", "lakehouse": "bronze"}
        assert detect_config_type(raw) == "params"

    def test_explicit_type_field(self):
        """Use explicit 'type' field if present."""
        raw = {"config_version": "1.0", "type": "thread", "sources": {}}
        assert detect_config_type(raw) == "thread"

    def test_undetectable_type(self):
        """Raise ConfigParseError if type cannot be detected."""
        raw = {}  # Empty dict with no config_version
        with pytest.raises(ConfigParseError) as exc_info:
            detect_config_type(raw)
        assert "Unable to detect config type" in str(exc_info.value)


class TestParserIntegration:
    """Integration tests using real fixtures."""

    def test_parse_valid_thread(self):
        """Parse and validate a complete thread config."""
        raw = parse_yaml(FIXTURES / "valid_thread.thread")
        version = extract_config_version(raw)
        config_type = detect_config_type(raw)
        validate_config_version(version, config_type)

        assert version == (1, 0)
        assert config_type == "thread"
        assert "sources" in raw

    def test_parse_valid_weave(self):
        """Parse and validate a complete weave config."""
        raw = parse_yaml(FIXTURES / "valid_weave.weave")
        version = extract_config_version(raw)
        config_type = detect_config_type(raw)
        validate_config_version(version, config_type)

        assert version == (1, 0)
        assert config_type == "weave"
        assert "threads" in raw

    def test_parse_valid_loom(self):
        """Parse and validate a complete loom config."""
        raw = parse_yaml(FIXTURES / "valid_loom.loom")
        version = extract_config_version(raw)
        config_type = detect_config_type(raw)
        validate_config_version(version, config_type)

        assert version == (1, 0)
        assert config_type == "loom"
        assert "weaves" in raw

    def test_missing_version_fails(self):
        """Config without version should fail."""
        raw = parse_yaml(FIXTURES / "missing_version.yaml")
        with pytest.raises(ConfigParseError):
            extract_config_version(raw)

    def test_bad_version_fails(self):
        """Config with unsupported version should fail."""
        raw = parse_yaml(FIXTURES / "bad_version.yaml")
        version = extract_config_version(raw)
        config_type = detect_config_type(raw)
        with pytest.raises(ConfigVersionError):
            validate_config_version(version, config_type)


class TestExtensionDetection:
    """Test detect_config_type_from_extension for typed extensions."""

    def test_thread_extension(self):
        assert detect_config_type_from_extension("dim_customer.thread") == "thread"

    def test_weave_extension(self):
        assert detect_config_type_from_extension("dimensions.weave") == "weave"

    def test_loom_extension(self):
        assert detect_config_type_from_extension("nightly.loom") == "loom"

    def test_yaml_returns_none(self):
        assert detect_config_type_from_extension("params.yaml") is None

    def test_yml_returns_none(self):
        assert detect_config_type_from_extension("params.yml") is None

    def test_unsupported_extension_raises(self):
        with pytest.raises(ConfigError, match="Unsupported extension"):
            detect_config_type_from_extension("file.json")

    def test_case_insensitive(self):
        assert detect_config_type_from_extension("file.THREAD") == "thread"
        assert detect_config_type_from_extension("file.Weave") == "weave"

    def test_warp_extension(self):
        assert detect_config_type_from_extension("dim_customer.warp") == "warp"

    def test_path_with_directories(self):
        assert detect_config_type_from_extension("dims/dim_customer.thread") == "thread"
