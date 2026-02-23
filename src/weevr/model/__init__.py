"""Domain object model for weevr configuration."""

from weevr.model.failure import FailureConfig
from weevr.model.keys import ChangeDetectionConfig, KeyConfig, SurrogateKeyConfig
from weevr.model.load import LoadConfig
from weevr.model.loom import Loom
from weevr.model.params import ParamsConfig, ParamSpec
from weevr.model.pipeline import (
    CastStep,
    DedupStep,
    DeriveStep,
    DropStep,
    FilterStep,
    JoinStep,
    RenameStep,
    SelectStep,
    SortStep,
    Step,
    UnionStep,
)
from weevr.model.source import DedupConfig, Source
from weevr.model.target import ColumnMapping, Target
from weevr.model.thread import Thread
from weevr.model.types import SparkExpr
from weevr.model.validation import Assertion, ValidationRule
from weevr.model.weave import Weave
from weevr.model.write import WriteConfig

__all__ = [
    # Top-level domain models
    "Thread",
    "Weave",
    "Loom",
    # Failure handling
    "FailureConfig",
    # Source
    "Source",
    "DedupConfig",
    # Target
    "Target",
    "ColumnMapping",
    # Pipeline steps
    "Step",
    "FilterStep",
    "DeriveStep",
    "JoinStep",
    "SelectStep",
    "DropStep",
    "RenameStep",
    "CastStep",
    "DedupStep",
    "SortStep",
    "UnionStep",
    # Keys
    "KeyConfig",
    "SurrogateKeyConfig",
    "ChangeDetectionConfig",
    # Write
    "WriteConfig",
    # Validation
    "ValidationRule",
    "Assertion",
    # Load
    "LoadConfig",
    # Params
    "ParamSpec",
    "ParamsConfig",
    # Types
    "SparkExpr",
]
