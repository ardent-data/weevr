"""Thread domain model."""

from typing import Any

from weevr.model.base import FrozenBase
from weevr.model.execution import ExecutionConfig
from weevr.model.failure import FailureConfig
from weevr.model.keys import KeyConfig
from weevr.model.load import LoadConfig
from weevr.model.params import ParamSpec
from weevr.model.pipeline import Step
from weevr.model.source import Source
from weevr.model.target import Target
from weevr.model.validation import Assertion, ValidationRule
from weevr.model.write import WriteConfig


class Thread(FrozenBase):
    """Complete domain model for a thread configuration.

    A thread is the smallest unit of work: one or more sources, a sequence
    of transformation steps, and a single target.
    """

    name: str = ""
    qualified_key: str = ""
    config_version: str
    sources: dict[str, Source]
    steps: list[Step] = []
    target: Target
    write: WriteConfig | None = None
    keys: KeyConfig | None = None
    validations: list[ValidationRule] | None = None
    assertions: list[Assertion] | None = None
    load: LoadConfig | None = None
    tags: list[str] | None = None
    params: dict[str, ParamSpec] | None = None
    defaults: dict[str, Any] | None = None
    failure: FailureConfig | None = None
    execution: ExecutionConfig | None = None
    cache: bool | None = None
