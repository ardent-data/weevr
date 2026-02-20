"""Thread execution result model."""

from typing import Literal

from weevr.model.base import FrozenBase


class ThreadResult(FrozenBase):
    """Immutable record of a completed thread execution.

    Attributes:
        status: Outcome of the execution — ``"success"`` or ``"failure"``.
        thread_name: Name of the thread that was executed.
        rows_written: Number of rows in the DataFrame at write time.
        write_mode: The write mode used (``"overwrite"``, ``"append"``, or ``"merge"``).
        target_path: Physical path of the Delta table that was written.
    """

    status: Literal["success", "failure"]
    thread_name: str
    rows_written: int
    write_mode: str
    target_path: str
