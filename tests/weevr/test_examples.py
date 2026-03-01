from __future__ import annotations

from pathlib import Path

import pytest
import yaml

EXAMPLES_DIR = Path(__file__).resolve().parents[2] / "examples"

EXAMPLE_FILES = sorted(
    p for ext in ("**/*.thread", "**/*.weave", "**/*.loom") for p in EXAMPLES_DIR.glob(ext)
)


def _config_type(data: dict) -> str:
    """Infer the config type from top-level keys."""
    if "sources" in data and "target" in data:
        return "thread"
    if "threads" in data:
        return "weave"
    if "weaves" in data:
        return "loom"
    return "unknown"


@pytest.mark.parametrize(
    "yml_path",
    EXAMPLE_FILES,
    ids=[p.stem for p in EXAMPLE_FILES],
)
class TestExampleYaml:
    def test_parses_as_valid_yaml(self, yml_path: Path) -> None:
        data = yaml.safe_load(yml_path.read_text())
        assert isinstance(data, dict), f"{yml_path.name} did not parse as a YAML mapping"

    def test_has_config_version(self, yml_path: Path) -> None:
        data = yaml.safe_load(yml_path.read_text())
        assert "config_version" in data, f"{yml_path.name} missing config_version"
        assert data["config_version"] == "1.0"

    def test_has_name(self, yml_path: Path) -> None:
        data = yaml.safe_load(yml_path.read_text())
        assert "name" in data, f"{yml_path.name} missing name"
        assert isinstance(data["name"], str) and len(data["name"]) > 0

    def test_thread_structure(self, yml_path: Path) -> None:
        data = yaml.safe_load(yml_path.read_text())
        if _config_type(data) != "thread":
            pytest.skip("not a thread config")
        assert "sources" in data, f"{yml_path.name} thread missing sources"
        assert isinstance(data["sources"], dict)
        assert len(data["sources"]) > 0
        assert "target" in data, f"{yml_path.name} thread missing target"
        assert isinstance(data["target"], dict)

    def test_weave_structure(self, yml_path: Path) -> None:
        data = yaml.safe_load(yml_path.read_text())
        if _config_type(data) != "weave":
            pytest.skip("not a weave config")
        assert "threads" in data, f"{yml_path.name} weave missing threads"
        assert isinstance(data["threads"], list)
        assert len(data["threads"]) > 0

    def test_loom_structure(self, yml_path: Path) -> None:
        data = yaml.safe_load(yml_path.read_text())
        if _config_type(data) != "loom":
            pytest.skip("not a loom config")
        assert "weaves" in data, f"{yml_path.name} loom missing weaves"
        assert isinstance(data["weaves"], list)
        assert len(data["weaves"]) > 0

    def test_at_least_one_example_discovered(self, yml_path: Path) -> None:
        assert len(EXAMPLE_FILES) > 0, "no typed extension example files found"
