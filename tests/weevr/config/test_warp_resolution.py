"""Load-time warp resolution: warps resolve from the thread file and project root."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from weevr.config import load_config, load_resolved
from weevr.config.locations import ConfigLocation
from weevr.config.warp import resolve_warp
from weevr.errors import ConfigError
from weevr.model.thread import Thread


def _warp(*columns: str) -> str:
    body = "".join(f"  - name: {c}\n    type: string\n" for c in columns)
    return f'config_version: "1.0"\ncolumns:\n{body}'


def _thread(target: dict[str, Any], name: str = "dim_customer") -> dict[str, Any]:
    return {
        "config_version": "1.0",
        "name": name,
        "sources": {"src": {"type": "delta", "alias": "raw.customers"}},
        "target": target,
    }


def _write(path: Path, data: dict[str, Any] | str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(data if isinstance(data, str) else yaml.safe_dump(data, sort_keys=False))
    return path


@pytest.fixture()
def project(tmp_path: Path) -> Path:
    return tmp_path / "proj.weevr"


def _load_thread(path: Path, root: Path) -> Thread:
    model = load_config(path, project_root=root)
    assert isinstance(model, Thread)
    return model


class TestExplicitWarpResolution:
    """An explicit target.warp resolves from the thread's directory, then the root."""

    def test_alias_target_resolves_from_thread_dir(self, project: Path):
        _write(project / "threads" / "dim_customer.warp", _warp("in_thread_dir"))
        _write(project / "dim_customer.warp", _warp("in_root"))
        path = _write(
            project / "threads" / "dim_customer.thread",
            _thread({"alias": "gold.dim_customer", "warp": "dim_customer"}),
        )
        binding = _load_thread(path, project).warp_binding
        assert binding is not None and binding.warp is not None
        assert [c.name for c in binding.warp.columns] == ["in_thread_dir"]
        assert binding.warp_file == str(project / "threads" / "dim_customer.warp")
        assert binding.thread_dir == str(project / "threads")
        assert binding.writable is True

    def test_falls_back_to_project_root(self, project: Path):
        _write(project / "dim_customer.warp", _warp("in_root"))
        path = _write(
            project / "threads" / "dim_customer.thread",
            _thread({"alias": "gold.dim_customer", "warp": "dim_customer"}),
        )
        binding = _load_thread(path, project).warp_binding
        assert binding is not None and binding.warp is not None
        assert [c.name for c in binding.warp.columns] == ["in_root"]

    def test_abfss_target_resolves(self, project: Path):
        _write(project / "threads" / "dim.warp", _warp("a"))
        target = {
            "path": "abfss://ws@onelake.dfs.fabric.microsoft.com/lh.Lakehouse/Tables/dim",
            "warp": "dim",
        }
        path = _write(project / "threads" / "dim_customer.thread", _thread(target))
        binding = _load_thread(path, project).warp_binding
        assert binding is not None and binding.warp is not None

    def test_name_with_separator_resolves_from_root(self, project: Path):
        _write(project / "warps" / "dim.warp", _warp("a"))
        path = _write(
            project / "threads" / "dim_customer.thread",
            _thread({"alias": "gold.dim_customer", "warp": "warps/dim"}),
        )
        binding = _load_thread(path, project).warp_binding
        assert binding is not None and binding.warp is not None

    def test_missing_warp_fails_at_load(self, project: Path):
        path = _write(
            project / "threads" / "dim_customer.thread",
            _thread({"alias": "gold.dim_customer", "warp": "nope"}),
        )
        with pytest.raises(ConfigError, match="Warp file not found: 'nope'") as exc:
            load_config(path, project_root=project)
        assert "dim_customer.thread" in str(exc.value)

    def test_opt_out_binds_no_warp(self, project: Path):
        _write(project / "threads" / "dim_customer.warp", _warp("a"))
        path = _write(
            project / "threads" / "dim_customer.thread",
            _thread({"alias": "gold.dim_customer", "warp": False}),
        )
        binding = _load_thread(path, project).warp_binding
        assert binding is not None
        assert binding.warp is None

    def test_hand_built_thread_has_no_binding(self):
        thread = Thread.model_validate(_thread({"alias": "gold.x", "warp": "x"}))
        assert thread.warp_binding is None


class TestChildThreadWarpResolution:
    """Threads pulled in by a weave resolve from their own file's directory."""

    def test_referenced_thread_uses_its_own_directory(self, project: Path):
        _write(project / "threads" / "dims" / "dim_customer.warp", _warp("child_dir"))
        _write(project / "dim_customer.warp", _warp("in_root"))
        _write(
            project / "threads" / "dims" / "dim_customer.thread",
            _thread({"alias": "gold.dim_customer", "warp": "dim_customer"}),
        )
        weave = _write(
            project / "w.weave",
            {"config_version": "1.0", "threads": [{"ref": "threads/dims/dim_customer.thread"}]},
        )
        loaded = load_resolved(weave, project_root=project)
        thread = loaded.resolved_threads["w"]["dim_customer"]
        assert thread.warp_binding.warp.columns[0].name == "child_dir"

    def test_inline_thread_uses_the_enclosing_file(self, project: Path):
        _write(project / "weaves" / "dim_customer.warp", _warp("beside_weave"))
        inline = _thread({"alias": "gold.dim_customer", "warp": "dim_customer"})
        weave = _write(
            project / "weaves" / "w.weave",
            {"config_version": "1.0", "threads": [inline]},
        )
        loaded = load_resolved(weave, project_root=project)
        thread = loaded.resolved_threads["w"]["dim_customer"]
        assert thread.warp_binding.warp.columns[0].name == "beside_weave"


class TestWarpAutoDiscovery:
    """With target.warp unset, a warp named after the target is discovered."""

    @pytest.mark.parametrize(
        ("target", "warp_name"),
        [
            ({"alias": "gold.dim_customer"}, "dim_customer"),
            ({"alias": "dim_customer"}, "dim_customer"),
            ({"path": "Tables/dim_customer"}, "dim_customer"),
            (
                {"path": "abfss://ws@onelake.dfs.fabric.microsoft.com/lh/Tables/dim_x/"},
                "dim_x",
            ),
            ({"connection": "lh", "table": "dim_conn"}, "dim_conn"),
        ],
    )
    def test_discovers_by_target_name(self, project: Path, target: dict, warp_name: str):
        _write(project / "threads" / f"{warp_name}.warp", _warp("found"))
        thread = _thread(target)
        if "connection" in target:
            thread["connections"] = {
                "lh": {"type": "onelake", "workspace": "ws", "lakehouse": "lh"}
            }
        path = _write(project / "threads" / "dim_customer.thread", thread)
        binding = _load_thread(path, project).warp_binding
        assert binding is not None and binding.warp is not None
        assert binding.warp_file == str(project / "threads" / f"{warp_name}.warp")

    def test_thread_dir_wins_over_project_root(self, project: Path):
        _write(project / "threads" / "dim_customer.warp", _warp("thread_dir"))
        _write(project / "dim_customer.warp", _warp("root"))
        path = _write(
            project / "threads" / "dim_customer.thread", _thread({"alias": "gold.dim_customer"})
        )
        binding = _load_thread(path, project).warp_binding
        assert binding.warp.columns[0].name == "thread_dir"

    def test_project_root_used_when_thread_dir_has_none(self, project: Path):
        _write(project / "dim_customer.warp", _warp("root"))
        path = _write(
            project / "threads" / "dim_customer.thread", _thread({"alias": "gold.dim_customer"})
        )
        binding = _load_thread(path, project).warp_binding
        assert binding.warp.columns[0].name == "root"

    def test_nothing_found_is_not_an_error(self, project: Path):
        path = _write(
            project / "threads" / "dim_customer.thread", _thread({"alias": "gold.dim_customer"})
        )
        binding = _load_thread(path, project).warp_binding
        assert binding is not None
        assert binding.warp is None and binding.warp_file is None

    def test_opt_out_skips_discovery(self, project: Path):
        _write(project / "threads" / "dim_customer.warp", _warp("found"))
        path = _write(
            project / "threads" / "dim_customer.thread",
            _thread({"alias": "gold.dim_customer", "warp": False}),
        )
        assert _load_thread(path, project).warp_binding.warp is None


class _MemoryLocation(ConfigLocation):
    """Non-local ConfigLocation over an in-memory file map (stands in for OneLake)."""

    def __init__(self, files: dict[str, str], path: str) -> None:
        self._files = files
        self._path = path.rstrip("/")

    def join(self, rel: str) -> ConfigLocation:
        return _MemoryLocation(self._files, f"{self._path}/{rel}")

    def exists(self) -> bool:
        return self._path in self._files or any(k.startswith(self._path + "/") for k in self._files)

    def read_text(self) -> str:
        return self._files[self._path]

    def is_relative_to(self, other: ConfigLocation) -> bool:
        return self._path.startswith(str(other))

    @property
    def name(self) -> str:
        return self._path.rsplit("/", 1)[-1]

    @property
    def stem(self) -> str:
        return self.name.rsplit(".", 1)[0]

    @property
    def suffix(self) -> str:
        return "." + self.name.rsplit(".", 1)[1] if "." in self.name else ""

    @property
    def parent(self) -> ConfigLocation:
        return _MemoryLocation(self._files, self._path.rsplit("/", 1)[0])

    def __str__(self) -> str:
        return self._path


class TestNonLocalLocations:
    """Resolution goes through ConfigLocation, so remote projects work."""

    def test_resolves_through_config_location(self):
        root = "mem://ws/lh/Files/proj.weevr"
        files = {f"{root}/dim_customer.warp": _warp("remote")}
        warp = resolve_warp(
            warp_ref="dim_customer",
            thread_dir=_MemoryLocation(files, f"{root}/threads"),
            config_root=_MemoryLocation(files, root),
        )
        assert warp is not None
        assert warp.columns[0].name == "remote"
