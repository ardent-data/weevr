"""Commit stamps: the HWM field and thread-level matching."""

import json

from weevr.operations.target_handle import CommitStamp, stamped_hwm


class TestCommitStampHwm:
    def test_hwm_serialized_when_set(self):
        stamp = CommitStamp(
            run_id="r1", thread="t", mode="append", hwm="2024-01-01T17:00:00.000000Z"
        )
        assert json.loads(stamp.to_json())["hwm"] == "2024-01-01T17:00:00.000000Z"

    def test_hwm_omitted_when_unset(self):
        assert "hwm" not in json.loads(CommitStamp(run_id="r1", thread="t").to_json())

    def test_hwm_does_not_change_own_commit_matching(self):
        stamp = CommitStamp(run_id="r1", thread="t", hwm="10")
        assert stamp.matches(stamp.to_json())
        assert not CommitStamp(run_id="r2", thread="t").matches(stamp.to_json())


class TestStampedHwm:
    def test_any_run_of_the_same_thread(self):
        meta = CommitStamp(run_id="older-run", thread="t", hwm="10").to_json()
        assert stamped_hwm(meta, "t") == "10"

    def test_other_thread_or_no_hwm_or_foreign_metadata(self):
        assert stamped_hwm(CommitStamp(run_id="r", thread="other", hwm="10").to_json(), "t") is None
        assert stamped_hwm(CommitStamp(run_id="r", thread="t").to_json(), "t") is None
        assert stamped_hwm('{"thread": "t", "hwm": "10"}', "t") is None
        assert stamped_hwm("not json", "t") is None
        assert stamped_hwm(None, "t") is None
