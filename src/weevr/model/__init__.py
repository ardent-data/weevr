"""Domain object model for weevr configuration."""

from weevr.model.execution import ExecutionConfig, LogLevel
from weevr.model.failure import FailureConfig
from weevr.model.hooks import (
    HookStep,
    LogMessageStep,
    QualityGateStep,
    SqlStatementStep,
)
from weevr.model.keys import ChangeDetectionConfig, KeyConfig, SurrogateKeyConfig
from weevr.model.load import LoadConfig
from weevr.model.lookup import Lookup
from weevr.model.loom import Loom, WeaveEntry
from weevr.model.params import ParamsConfig, ParamSpec
from weevr.model.pipeline import (
    AggregateStep,
    CaseWhenStep,
    CastStep,
    CoalesceStep,
    DateOpsStep,
    DedupStep,
    DeriveStep,
    DropStep,
    FillNullStep,
    FilterStep,
    JoinStep,
    PivotStep,
    RenameStep,
    SelectStep,
    SortStep,
    Step,
    StringOpsStep,
    UnionStep,
    UnpivotStep,
    WindowStep,
)
from weevr.model.source import DedupConfig, Source
from weevr.model.target import ColumnMapping, Target
from weevr.model.thread import Thread
from weevr.model.types import SparkExpr
from weevr.model.validation import Assertion, ValidationRule
from weevr.model.variable import VariableSpec
from weevr.model.weave import ConditionSpec, ThreadEntry, Weave
from weevr.model.write import WriteConfig

__all__ = [
    # Top-level domain models
    "Thread",
    "Weave",
    "Loom",
    # Failure handling
    "FailureConfig",
    # Weave thread entry
    "ThreadEntry",
    "ConditionSpec",
    # Loom weave entry
    "WeaveEntry",
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
    "AggregateStep",
    "WindowStep",
    "PivotStep",
    "UnpivotStep",
    "CaseWhenStep",
    "FillNullStep",
    "CoalesceStep",
    "StringOpsStep",
    "DateOpsStep",
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
    # Execution config
    "ExecutionConfig",
    "LogLevel",
    # Types
    "SparkExpr",
    # Hooks
    "HookStep",
    "QualityGateStep",
    "SqlStatementStep",
    "LogMessageStep",
    # Lookup
    "Lookup",
    # Variable
    "VariableSpec",
]
