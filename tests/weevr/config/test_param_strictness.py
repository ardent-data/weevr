"""Lenient warnings and the ``strict_params`` switch.

A ``${param.x}`` bound but declared nowhere in its scope chain warns ``P120``
with a paste-able ``params:`` block; a runtime key nothing declares or
references warns ``P121``. ``strict_params: true`` on a loom, weave or thread
document (most specific wins) makes both errors, and the load API's
``strict_params`` beats every document. A reference with no binding and no
fallback fails ``P110`` whatever the mode.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from documents import write_document
from weevr.config import load_config, load_resolved
from weevr.config.loader import collect_resolved_config
from weevr.config.locations import LocalConfigLocation
from weevr.diagnostics import Diagnostic
from weevr.errors import ConfigError

THREAD: dict[str, Any] = {
    "config_version": "2.0",
    "sources": {"s": {"type": "delta", "alias": "raw.t"}},
    "target": {"alias": "out.${param.x}"},
}


def _spec(name: str, **extra: Any) -> dict[str, Any]:
    return {name: {"name": name, "type": "string", **extra}}


def _thread(**extra: Any) -> dict[str, Any]:
    return {**THREAD, **extra}


def _weave(threads: list[Any], **extra: Any) -> dict[str, Any]:
    return {"config_version": "2.0", "threads": threads, **extra}


def _loom(weaves: list[Any], **extra: Any) -> dict[str, Any]:
    return {"config_version": "2.0", "weaves": weaves, **extra}


def _write(root: Path, name: str, body: dict[str, Any]) -> Path:
    return write_document(root / name, body)


def _load(path: Path, runtime: dict[str, Any] | None = None, **kwargs: Any) -> Any:
    return load_resolved(path, runtime_params=runtime, project_root=path.parent, **kwargs)


def _with_code(diagnostics: list[Diagnostic], code: str) -> list[Diagnostic]:
    return [d for d in diagnostics if d.code == code]


def _failure(path: Path, runtime: dict[str, Any] | None = None, **kwargs: Any) -> ConfigError:
    with pytest.raises(ConfigError) as info:
        _load(path, runtime, **kwargs)
    return info.value


def _collected(
    path: Path, runtime: dict[str, Any] | None = None, **kwargs: Any
) -> tuple[Any, list[Diagnostic], list[Diagnostic]]:
    return collect_resolved_config(
        LocalConfigLocation(path),
        runtime_params=runtime,
        project_root=LocalConfigLocation(path.parent),
        **kwargs,
    )


class TestUndeclaredParameterLenient:
    """``P120``: a bound reference no declaration in its chain covers warns."""

    def test_runtime_bound_undeclared_warns(self, tmp_path: Path) -> None:
        path = _write(tmp_path, "t.thread", _thread())
        loaded = _load(path, {"x": "a"})
        (warning,) = _with_code(loaded.diagnostics, "P120")
        assert not warning.is_error
        assert warning.file == str(path)
        assert warning.pointer == "/target/alias"
        assert loaded.model.target.alias == "out.a"

    def test_one_per_name_with_every_file_name_in_the_hint(self, tmp_path: Path) -> None:
        body = _thread(
            target={"alias": "out.${param.x}"},
            sources={"s": {"type": "delta", "alias": "raw.${param.y}"}},
        )
        path = _write(tmp_path, "t.thread", body)
        warnings = _with_code(_load(path, {"x": "a", "y": "b"}).diagnostics, "P120")
        assert sorted(d.pointer or "" for d in warnings) == ["/sources/s/alias", "/target/alias"]
        for warning in warnings:
            assert warning.hint is not None
            assert "params:" in warning.hint
            assert "name: x" in warning.hint
            assert "name: y" in warning.hint

    def test_hint_declares_the_bound_value_type(self, tmp_path: Path) -> None:
        path = _write(tmp_path, "t.thread", _thread(target={"alias": "out.t${param.n}"}))
        (warning,) = _with_code(_load(path, {"n": 3}).diagnostics, "P120")
        assert warning.hint is not None
        assert "type: int" in warning.hint

    def test_declared_in_ancestor_is_quiet(self, tmp_path: Path) -> None:
        _write(tmp_path, "t.thread", _thread())
        path = _write(tmp_path, "w.weave", _weave([{"ref": "t.thread"}], params=_spec("x")))
        assert _with_code(_load(path, {"x": "a"}).diagnostics, "P120") == []

    def test_pinned_undeclared_warns_in_the_child_file(self, tmp_path: Path) -> None:
        thread = _write(tmp_path, "t.thread", _thread())
        path = _write(tmp_path, "w.weave", _weave([{"ref": "t.thread", "params": {"x": "p"}}]))
        (warning,) = _with_code(_load(path).diagnostics, "P120")
        assert warning.file == str(thread)

    def test_unbound_reference_with_fallback_is_quiet(self, tmp_path: Path) -> None:
        path = _write(tmp_path, "t.thread", _thread(target={"alias": "out.${param.x:-d}"}))
        assert _with_code(_load(path).diagnostics, "P120") == []

    def test_dotted_reference_names_its_head(self, tmp_path: Path) -> None:
        path = _write(tmp_path, "t.thread", _thread(target={"alias": "out.${param.env.db}"}))
        (warning,) = _with_code(_load(path, {"env": {"db": "d"}}).diagnostics, "P120")
        assert warning.hint is not None
        assert "name: env" in warning.hint


class TestUndeclaredParameterStrict:
    """``strict_params: true`` makes ``P120`` an error."""

    def test_strict_thread_fails_the_load(self, tmp_path: Path) -> None:
        path = _write(tmp_path, "t.thread", _thread(strict_params=True))
        error = _failure(path, {"x": "a"})
        (found,) = _with_code(error.diagnostics, "P120")
        assert found.is_error

    def test_strict_declared_is_quiet(self, tmp_path: Path) -> None:
        path = _write(tmp_path, "t.thread", _thread(strict_params=True, params=_spec("x")))
        assert _load(path, {"x": "a"}).model.target.alias == "out.a"

    def test_validate_records_the_error(self, tmp_path: Path) -> None:
        path = _write(tmp_path, "t.thread", _thread(strict_params=True))
        document, warnings, errors = _collected(path, {"x": "a"})
        assert document is not None
        assert [d.code for d in errors] == ["P120"]
        assert _with_code(warnings, "P120") == []


class TestUnsuppliedReference:
    """An undeclared and unsupplied reference fails ``P110`` in both modes."""

    @pytest.mark.parametrize("strict", [False, True])
    def test_p110(self, tmp_path: Path, strict: bool) -> None:
        path = _write(tmp_path, "t.thread", _thread(strict_params=strict))
        error = _failure(path)
        assert [d.code for d in error.diagnostics] == ["P110"]


class TestUnusedSuppliedParameter:
    """``P121``: a runtime key nothing declares or references."""

    def test_unused_runtime_key_warns(self, tmp_path: Path) -> None:
        path = _write(tmp_path, "t.thread", _thread(params=_spec("x")))
        loaded = _load(path, {"x": "a", "extra": 1})
        (warning,) = _with_code(loaded.diagnostics, "P121")
        assert not warning.is_error
        assert warning.hint is not None
        assert "extra" in warning.hint

    def test_referenced_key_is_quiet(self, tmp_path: Path) -> None:
        path = _write(tmp_path, "t.thread", _thread())
        assert _with_code(_load(path, {"x": "a"}).diagnostics, "P121") == []

    def test_declared_unreferenced_key_is_quiet(self, tmp_path: Path) -> None:
        path = _write(tmp_path, "t.thread", _thread(params={**_spec("x"), **_spec("y")}))
        assert _with_code(_load(path, {"x": "a", "y": "b"}).diagnostics, "P121") == []

    def test_key_read_in_a_child_file_is_quiet(self, tmp_path: Path) -> None:
        _write(tmp_path, "t.thread", _thread())
        path = _write(tmp_path, "w.weave", _weave([{"ref": "t.thread"}]))
        assert _with_code(_load(path, {"x": "a"}).diagnostics, "P121") == []

    def test_key_read_only_by_a_pin_is_quiet(self, tmp_path: Path) -> None:
        _write(tmp_path, "t.thread", _thread())
        entry = {"ref": "t.thread", "params": {"x": "${param.y}"}}
        path = _write(tmp_path, "w.weave", _weave([entry]))
        assert _with_code(_load(path, {"y": "a"}).diagnostics, "P121") == []

    def test_key_read_in_an_inline_thread_is_quiet(self, tmp_path: Path) -> None:
        path = _write(tmp_path, "w.weave", _weave([{**_thread(), "name": "i"}]))
        assert _with_code(_load(path, {"x": "a"}).diagnostics, "P121") == []

    def test_none_keys_are_quiet(self, tmp_path: Path) -> None:
        path = _write(tmp_path, "t.thread", _thread(params=_spec("x")))
        runtime = {"x": "a", "unset": None}
        assert _with_code(_load(path, runtime).diagnostics, "P121") == []

    def test_strict_root_fails_the_load(self, tmp_path: Path) -> None:
        path = _write(tmp_path, "t.thread", _thread(strict_params=True, params=_spec("x")))
        error = _failure(path, {"x": "a", "extra": 1})
        (found,) = _with_code(error.diagnostics, "P121")
        assert found.is_error

    def test_strict_validate_records_the_error(self, tmp_path: Path) -> None:
        path = _write(tmp_path, "t.thread", _thread(strict_params=True, params=_spec("x")))
        document, _, errors = _collected(path, {"x": "a", "extra": 1})
        assert document is not None
        assert [d.code for d in errors] == ["P121"]

    def test_strictness_is_the_roots(self, tmp_path: Path) -> None:
        _write(tmp_path, "t.thread", _thread(strict_params=True, params=_spec("x")))
        path = _write(tmp_path, "w.weave", _weave([{"ref": "t.thread"}]))
        (warning,) = _with_code(_load(path, {"x": "a", "extra": 1}).diagnostics, "P121")
        assert not warning.is_error


class TestMostSpecificWins:
    """The nearest ``strict_params`` in a reference's chain decides its severity."""

    def _tree(self, tmp_path: Path, loom: Any, weave: Any, thread: Any) -> Path:
        def flag(value: Any) -> dict[str, Any]:
            return {} if value is None else {"strict_params": value}

        _write(tmp_path, "t.thread", _thread(**flag(thread)))
        _write(tmp_path, "w.weave", _weave([{"ref": "t.thread"}], **flag(weave)))
        return _write(tmp_path, "l.loom", _loom([{"ref": "w.weave"}], **flag(loom)))

    @pytest.mark.parametrize(
        ("loom", "weave", "thread", "strict"),
        [
            (None, None, None, False),
            (True, None, None, True),
            (True, False, None, False),
            (True, None, False, False),
            (False, True, None, True),
            (None, None, True, True),
            (False, False, True, True),
        ],
    )
    def test_ref_chain(
        self, tmp_path: Path, loom: Any, weave: Any, thread: Any, strict: bool
    ) -> None:
        path = self._tree(tmp_path, loom, weave, thread)
        document, warnings, errors = _collected(path, {"x": "a"})
        assert document is not None
        found = _with_code(errors if strict else warnings, "P120")
        assert [d.is_error for d in found] == [strict]

    def test_inline_thread_setting_applies_to_its_body(self, tmp_path: Path) -> None:
        inline = {**_thread(), "name": "i", "strict_params": True}
        path = _write(tmp_path, "w.weave", _weave([inline]))
        error = _failure(path, {"x": "a"})
        (found,) = _with_code(error.diagnostics, "P120")
        assert found.pointer == "/threads/0/target/alias"

    def test_inline_weave_setting_applies_to_its_body(self, tmp_path: Path) -> None:
        inline_thread = {**_thread(), "name": "i"}
        inline_weave = {**_weave([inline_thread]), "name": "w", "strict_params": True}
        path = _write(tmp_path, "l.loom", _loom([inline_weave]))
        (found,) = _with_code(_failure(path, {"x": "a"}).diagnostics, "P120")
        assert found.is_error

    def test_lenient_inline_child_under_strict_parent(self, tmp_path: Path) -> None:
        inline = {**_thread(), "name": "i", "strict_params": False}
        path = _write(tmp_path, "w.weave", _weave([inline], strict_params=True))
        (warning,) = _with_code(_load(path, {"x": "a"}).diagnostics, "P120")
        assert not warning.is_error


class TestApiOverride:
    """The load API's ``strict_params`` beats every document, both ways."""

    def test_true_escalates_lenient_documents(self, tmp_path: Path) -> None:
        path = _write(tmp_path, "t.thread", _thread())
        error = _failure(path, {"x": "a", "extra": 1}, strict_params=True)
        assert sorted(d.code for d in error.diagnostics) == ["P120", "P121"]

    def test_false_relaxes_strict_documents(self, tmp_path: Path) -> None:
        _write(tmp_path, "t.thread", _thread(strict_params=True))
        path = _write(tmp_path, "w.weave", _weave([{"ref": "t.thread"}], strict_params=True))
        loaded = _load(path, {"x": "a", "extra": 1}, strict_params=False)
        codes = sorted(d.code for d in loaded.diagnostics if d.code in ("P120", "P121"))
        assert codes == ["P120", "P121"]
        assert not any(d.is_error for d in loaded.diagnostics)

    def test_override_reaches_collect(self, tmp_path: Path) -> None:
        path = _write(tmp_path, "t.thread", _thread())
        _, _, errors = _collected(path, {"x": "a"}, strict_params=True)
        assert [d.code for d in errors] == ["P120"]

    def test_load_config_true(self, tmp_path: Path) -> None:
        path = _write(tmp_path, "t.thread", _thread())
        with pytest.raises(ConfigError) as info:
            load_config(path, {"x": "a"}, project_root=tmp_path, strict_params=True)
        assert [d.code for d in info.value.diagnostics] == ["P120"]

    def test_load_config_false(self, tmp_path: Path) -> None:
        path = _write(tmp_path, "t.thread", _thread(strict_params=True))
        model = load_config(path, {"x": "a"}, project_root=tmp_path, strict_params=False)
        assert model.target.alias == "out.a"  # type: ignore[union-attr]

    @pytest.mark.parametrize("strict", [False, True])
    def test_p110_regardless(self, tmp_path: Path, strict: bool) -> None:
        path = _write(tmp_path, "t.thread", _thread())
        error = _failure(path, strict_params=strict)
        assert [d.code for d in error.diagnostics] == ["P110"]


class TestModel:
    """``strict_params`` is an optional bool on each component document."""

    @pytest.mark.parametrize("name", ["t.thread", "w.weave", "l.loom"])
    def test_absent_is_none(self, tmp_path: Path, name: str) -> None:
        _write(tmp_path, "t.thread", _thread(params=_spec("x")))
        _write(tmp_path, "w.weave", _weave([{"ref": "t.thread"}]))
        _write(tmp_path, "l.loom", _loom([{"ref": "w.weave"}]))
        assert _load(tmp_path / name, {"x": "a"}).model.strict_params is None

    def test_set_is_kept(self, tmp_path: Path) -> None:
        path = _write(tmp_path, "t.thread", _thread(strict_params=False))
        assert _load(path, {"x": "a"}).model.strict_params is False

    def test_non_bool_is_rejected(self, tmp_path: Path) -> None:
        path = _write(tmp_path, "t.thread", _thread(strict_params="yes please"))
        with pytest.raises(ConfigError):
            _load(path, {"x": "a"})
