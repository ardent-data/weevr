"""Tests for attaching the source file to config errors."""

from pathlib import Path

import pytest

from weevr.config.attribution import attribute_errors
from weevr.config.locations import LocalConfigLocation
from weevr.errors import ConfigError, ConfigSchemaError


class TestAttributeErrors:
    """attribute_errors names the file on config errors leaving its block."""

    def test_unattributed_error_gets_location(self):
        """A ConfigError without a file_path gets the block's location."""
        location = LocalConfigLocation(Path("/proj/a.thread"))
        with pytest.raises(ConfigSchemaError) as exc_info, attribute_errors(location):
            raise ConfigSchemaError("bad shape")
        assert exc_info.value.file_path == "/proj/a.thread"

    def test_existing_file_path_is_kept(self):
        """A ConfigError that already names a file keeps it."""
        location = LocalConfigLocation(Path("/proj/a.thread"))
        with pytest.raises(ConfigError) as exc_info, attribute_errors(location):
            raise ConfigError("missing", file_path="/proj/other.thread")
        assert exc_info.value.file_path == "/proj/other.thread"

    def test_nested_blocks_keep_inner_location(self):
        """When blocks nest, the innermost location wins."""
        outer = LocalConfigLocation(Path("/proj/w.weave"))
        inner = LocalConfigLocation(Path("/proj/t.thread"))
        with (
            pytest.raises(ConfigError) as exc_info,
            attribute_errors(outer),
            attribute_errors(inner),
        ):
            raise ConfigError("bad thread")
        assert exc_info.value.file_path == "/proj/t.thread"

    def test_other_exceptions_pass_through(self):
        """Exceptions that aren't ConfigError propagate unchanged."""
        location = LocalConfigLocation(Path("/proj/a.thread"))
        original = ValueError("not a config error")
        with pytest.raises(ValueError) as exc_info, attribute_errors(location):
            raise original
        assert exc_info.value is original
        assert not hasattr(exc_info.value, "file_path")
