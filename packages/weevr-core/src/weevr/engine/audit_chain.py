"""Warn when a thread repeats the audit columns of the thread it reads from.

A thread that reads another thread's target finds the audit columns already
there; adding them again fails at run time, after the reads. The edges and
both threads' effective audit columns are known at load, so the overlap is
reported then. In a loom the edges span weaves: a thread in a later weave
that reads an earlier weave's target is checked too. Spark-free.
"""

from __future__ import annotations

from weevr.config.document import ResolvedDocument
from weevr.config.validation import in_thread
from weevr.diagnostics import Diagnostic, diagnostic
from weevr.engine.planner import infer_thread_edges
from weevr.model import Thread


def _threads_of(document: ResolvedDocument) -> dict[str, tuple[str, str | None, Thread]]:
    """Every thread under ``document`` as ``weave/thread`` key → (name, file, model).

    The key keeps names unique across the weaves of a loom.
    """
    if document.kind == "weave":
        weaves = {document.name: document}
    elif document.kind == "loom":
        weaves = dict(document.children)
    else:
        return {}
    return {
        f"{weave_name}/{name}": (name, node.source_ref or None, node.model)
        for weave_name, weave in weaves.items()
        for name, node in weave.children.items()
        if isinstance(node.model, Thread)
    }


def audit_chain_diagnostics(document: ResolvedDocument) -> list[Diagnostic]:
    """Find threads that would re-add the audit columns of a thread they read.

    For each inferred upstream to downstream edge, across every weave of the
    loaded tree, the downstream thread's effective audit columns (after the
    cascade and its exclusions) are compared with the upstream thread's. A
    downstream source that projects the columns away still warns, so this
    is a warning.

    Args:
        document: The loaded tree.

    Returns:
        One ``R150`` per overlapping edge, on the downstream thread, listing
        the overlapping column names.
    """
    nodes = _threads_of(document)
    models = {key: model for key, (_, _, model) in nodes.items()}
    found: list[Diagnostic] = []
    for key, upstream_keys in infer_thread_edges(models).items():
        mine = models[key].target.audit_columns
        if not mine:
            continue
        name, file, _ = nodes[key]
        for upstream_key in upstream_keys:
            theirs = models[upstream_key].target.audit_columns or {}
            overlap = [column for column in mine if column in theirs]
            if not overlap:
                continue
            upstream_name = nodes[upstream_key][0]
            found.extend(
                in_thread(
                    [
                        diagnostic(
                            "R150",
                            f"reads the target of thread '{upstream_name}', which already "
                            f"carries audit columns {overlap}; adding them again fails at run time",
                            resolved_path="/target/audit_columns",
                            expected=overlap,
                            hint="add them to target.audit_columns_exclude on this thread",
                        )
                    ],
                    name,
                    file=file,
                )
            )
    return found
