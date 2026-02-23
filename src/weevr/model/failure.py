"""Failure handling configuration model."""

from typing import Literal

from weevr.model.base import FrozenBase


class FailureConfig(FrozenBase):
    """Per-thread failure handling policy.

    Controls what happens to remaining threads in a weave when this thread fails.

    Attributes:
        on_failure: One of ``"abort_weave"`` (default), ``"skip_downstream"``,
            or ``"continue"``. See SPEC §14.2 for semantics.
    """

    on_failure: Literal["skip_downstream", "continue", "abort_weave"] = "abort_weave"
