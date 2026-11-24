"""Tests for YAML parser."""

from pathlib import Path

import pytest

from documents import write_document
from weevr.config import load_config
from weevr.config.parser import (
    SUPPORTED_VERSIONS,
    declared_kind,
    detect_config_type_from_extension,
    document_kind,
    extract_config_version,
    parse_yaml,
    read_envelope,
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

    @pytest.mark.parametrize("kind", ["thread", "weave", "loom", "warp", "params"])
    def test_v1_is_unsupported(self, kind):
        """A 1.x version is a v1 document, for every kind."""
        with pytest.raises(ConfigVersionError) as exc_info:
            validate_config_version((1, 0), kind)
        assert _only_diagnostic(exc_info.value).code == "C203"

    def test_thread_version_2_5(self):
        """Accept a later minor of the current major."""
        validate_config_version((2, 5), "thread")  # Should not raise

    def test_thread_unsupported_major(self):
        """Raise ConfigVersionError for unsupported major version."""
        with pytest.raises(ConfigVersionError) as exc_info:
            validate_config_version((3, 0), "thread")
        assert _only_diagnostic(exc_info.value).code == "C203"

    def test_unknown_config_type(self):
        """Raise ConfigVersionError for unknown config type."""
        with pytest.raises(ConfigVersionError) as exc_info:
            validate_config_version((2, 0), "unknown")
        assert _only_diagnostic(exc_info.value).code == "C204"


class TestParserIntegration:
    """Integration tests using real fixtures."""

    @pytest.mark.parametrize(
        ("name", "kind"),
        [
            ("valid_thread.thread", "thread"),
            ("valid_weave.weave", "weave"),
            ("valid_loom.loom", "loom"),
        ],
    )
    def test_envelope_of_valid_fixture(self, name, kind):
        """A valid fixture's envelope names its kind."""
        location = FIXTURES / name
        raw = parse_yaml(location)
        assert read_envelope(raw, detect_config_type_from_extension(location)) == kind

    def test_missing_version_fails(self):
        """Config without version fails with C201."""
        raw = parse_yaml(FIXTURES / "missing_version.yaml")
        with pytest.raises(ConfigParseError) as exc_info:
            read_envelope(raw, None)
        assert _only_diagnostic(exc_info.value).code == "C201"

    def test_bad_version_fails(self):
        """Config with unsupported version fails with C203."""
        raw = parse_yaml(FIXTURES / "bad_version.yaml")
        with pytest.raises(ConfigVersionError) as exc_info:
            read_envelope(raw, None)
        assert _only_diagnostic(exc_info.value).code == "C203"


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


def _only_diagnostic(exc: ConfigError):
    assert len(exc.diagnostics) == 1
    return exc.diagnostics[0]


class TestParseDiagnostics:
    """Every read, parse, version and type failure carries one coded diagnostic."""

    def test_file_not_found(self):
        with pytest.raises(ConfigParseError) as exc_info:
            parse_yaml(FIXTURES / "nonexistent.yaml")
        diag = _only_diagnostic(exc_info.value)
        assert diag.code == "C001"
        assert diag.file == str(FIXTURES / "nonexistent.yaml")

    def test_unreadable_path(self, tmp_path):
        with pytest.raises(ConfigParseError) as exc_info:
            parse_yaml(tmp_path)
        assert _only_diagnostic(exc_info.value).code == "C002"

    def test_not_utf8(self, tmp_path):
        path = tmp_path / "latin.thread"
        path.write_bytes(b"config_version: '1.0'\nname: caf\xe9\n")
        with pytest.raises(ConfigParseError) as exc_info:
            parse_yaml(path)
        diag = _only_diagnostic(exc_info.value)
        assert diag.code == "C003"
        assert diag.file == str(path)

    def test_empty_file(self, tmp_path):
        path = tmp_path / "empty.yaml"
        path.write_text("")
        with pytest.raises(ConfigParseError) as exc_info:
            parse_yaml(path)
        diag = _only_diagnostic(exc_info.value)
        assert diag.code == "C004"
        assert diag.file == str(path)

    def test_yaml_syntax_carries_line_and_column(self, tmp_path):
        path = tmp_path / "broken.thread"
        path.write_text("config_version: '2.0'\nsources:\n  a: [1, 2\n")
        with pytest.raises(ConfigParseError) as exc_info:
            parse_yaml(path)
        diag = _only_diagnostic(exc_info.value)
        assert diag.code == "C005"
        assert diag.file == str(path)
        assert diag.line == 4
        assert diag.column == 1

    def test_non_mapping(self, tmp_path):
        path = tmp_path / "list.yaml"
        path.write_text("- item1\n")
        with pytest.raises(ConfigParseError) as exc_info:
            parse_yaml(path)
        diag = _only_diagnostic(exc_info.value)
        assert diag.code == "C006"
        assert diag.pointer == ""

    def test_unsupported_extension(self):
        with pytest.raises(ConfigError) as exc_info:
            detect_config_type_from_extension("config.json")
        diag = _only_diagnostic(exc_info.value)
        assert diag.code == "C007"
        assert diag.file == "config.json"
        assert diag.expected == (".thread", ".weave", ".loom", ".warp", ".yaml", ".yml")

    @pytest.mark.parametrize(
        ("raw", "code"),
        [
            ({"sources": {}}, "C201"),
            ({"config_version": 1.0}, "C202"),
            ({"config_version": "10"}, "C202"),
            ({"config_version": "a.b"}, "C202"),
        ],
    )
    def test_config_version(self, raw, code):
        with pytest.raises(ConfigParseError) as exc_info:
            extract_config_version(raw)
        diag = _only_diagnostic(exc_info.value)
        assert diag.code == code
        assert diag.pointer == ("" if code == "C201" else "/config_version")

    def test_unsupported_major(self):
        with pytest.raises(ConfigVersionError) as exc_info:
            validate_config_version((3, 0), "thread")
        diag = _only_diagnostic(exc_info.value)
        assert diag.code == "C203"
        assert diag.pointer == "/config_version"
        assert diag.expected == ("2.x",)

    def test_unknown_config_type(self):
        with pytest.raises(ConfigVersionError) as exc_info:
            validate_config_version((2, 0), "pipeline")
        diag = _only_diagnostic(exc_info.value)
        assert diag.code == "C204"
        assert diag.expected == ("project", "loom", "weave", "thread", "warp", "params")

    def test_load_names_the_file_on_version_errors(self, tmp_path):
        path = tmp_path / "t.thread"
        write_document(path, "config_version: '9.0'\nsources: {}\n")
        with pytest.raises(ConfigVersionError) as exc_info:
            load_config(path)
        diag = _only_diagnostic(exc_info.value)
        assert diag.code == "C203"
        assert diag.file == str(path)


class TestDocumentKind:
    """The envelope's ``kind`` decides a document's type ahead of its extension."""

    @pytest.mark.parametrize("kind", ["project", "loom", "weave", "thread", "warp", "params"])
    def test_every_kind_reads_major_2(self, kind):
        assert SUPPORTED_VERSIONS[kind] == (2, 0)
        validate_config_version((2, 0), kind)

    def test_declared_kind_wins_for_yaml(self):
        raw = {"kind": "params", "config_version": "2.0", "threads": []}
        assert document_kind(raw, None) == "params"

    def test_declared_kind_matching_extension(self):
        raw = {"kind": "weave", "config_version": "2.0", "threads": []}
        assert document_kind(raw, "weave") == "weave"

    @pytest.mark.parametrize("ext_type", ["weave", None])
    def test_without_kind_is_missing_kind(self, ext_type):
        """Neither the extension nor the structure stands in for a missing kind."""
        raw = {"config_version": "2.0", "threads": []}
        with pytest.raises(ConfigParseError) as exc_info:
            document_kind(raw, ext_type)
        diag = _only_diagnostic(exc_info.value)
        assert diag.code == "C206"
        assert diag.pointer == ""
        assert diag.hint is not None and "weevr migrate" in diag.hint

    def test_kind_extension_mismatch(self):
        raw = {"kind": "weave", "config_version": "2.0", "threads": []}
        with pytest.raises(ConfigError) as exc_info:
            document_kind(raw, "thread")
        diag = _only_diagnostic(exc_info.value)
        assert diag.code == "C207"
        assert diag.pointer == "/kind"
        assert diag.expected == ("thread",)

    @pytest.mark.parametrize("kind", ["pipeline", 3, None])
    def test_unknown_kind(self, kind):
        raw = {"kind": kind, "config_version": "2.0"}
        with pytest.raises(ConfigError) as exc_info:
            document_kind(raw, None)
        diag = _only_diagnostic(exc_info.value)
        assert diag.code == "C204"
        assert diag.pointer == "/kind"
        assert diag.expected == ("project", "loom", "weave", "thread", "warp", "params")


class TestEnvelope:
    """``read_envelope`` checks the version before the kind before the extension."""

    @pytest.mark.parametrize("version", ["1.0", "1.5", "0.9"])
    @pytest.mark.parametrize("kind", [None, "thread"])
    def test_v1_document_is_the_migration_error(self, version, kind):
        """A pre-2 document fails C203 with the migrate hint, with or without kind."""
        raw: dict = {"config_version": version, "threads": []}
        if kind is not None:
            raw["kind"] = kind
        with pytest.raises(ConfigVersionError) as exc_info:
            read_envelope(raw, None)
        diag = _only_diagnostic(exc_info.value)
        assert diag.code == "C203"
        assert diag.pointer == "/config_version"
        assert diag.hint is not None and "weevr migrate" in diag.hint

    def test_v1_with_unknown_kind_is_the_migration_error(self):
        raw = {"config_version": "1.0", "kind": "pipeline"}
        with pytest.raises(ConfigVersionError) as exc_info:
            read_envelope(raw, "thread")
        assert _only_diagnostic(exc_info.value).code == "C203"

    def test_newer_major_has_no_migration_hint(self):
        with pytest.raises(ConfigVersionError) as exc_info:
            read_envelope({"config_version": "3.0", "kind": "thread"}, None)
        diag = _only_diagnostic(exc_info.value)
        assert diag.code == "C203"
        assert diag.hint is None or "weevr migrate" not in diag.hint

    def test_missing_version_before_kind(self):
        with pytest.raises(ConfigParseError) as exc_info:
            read_envelope({"threads": []}, None)
        assert _only_diagnostic(exc_info.value).code == "C201"

    def test_v2_without_kind(self):
        with pytest.raises(ConfigParseError) as exc_info:
            read_envelope({"config_version": "2.0", "threads": []}, "weave")
        assert _only_diagnostic(exc_info.value).code == "C206"

    def test_kind_before_extension_match(self):
        with pytest.raises(ConfigError) as exc_info:
            read_envelope({"config_version": "2.0", "kind": "weave"}, "thread")
        assert _only_diagnostic(exc_info.value).code == "C207"

    @pytest.mark.parametrize("kind", ["project", "loom", "weave", "thread", "warp", "params"])
    def test_v2_document_returns_its_kind(self, kind):
        assert read_envelope({"config_version": "2.0", "kind": kind}, None) == kind


class TestDeclaredKind:
    """``declared_kind`` reads a document's kind for display, never raising."""

    def test_declared(self, tmp_path):
        path = tmp_path / "t.yaml"
        path.write_text("kind: weave\nconfig_version: '1.0'\n")
        assert declared_kind(path) == "weave"

    @pytest.mark.parametrize("text", ["config_version: '2.0'\n", "kind: pipeline\n", "a: [1\n"])
    def test_none_when_absent_unknown_or_unreadable(self, tmp_path, text):
        path = tmp_path / "t.yaml"
        path.write_text(text)
        assert declared_kind(path) is None
