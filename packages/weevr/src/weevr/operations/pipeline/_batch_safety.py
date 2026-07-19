"""Sibling-reference scan for batchable per-column expressions.

Batched ``withColumns`` evaluates every expression against the pre-batch
frame; sequential ``withColumn`` lets expression N read output N-1. A
user-supplied expression fragment that names another column in the same
loop therefore changes meaning under batching. This scan decides, per
loop, whether batching is provably safe.

The invariant is directional: a false positive merely costs the
optimization (the loop falls back to sequential); a false negative would
silently change semantics and must be impossible. Two normalization
rules follow from that:

- **Case-insensitive comparison** — Spark resolves unquoted identifiers
  case-insensitively by default, so ``concat({col}, Backup_Col)``
  references ``backup_col``. Both sides are case-folded.
- **Backtick-aware tokenization** — column names are arbitrary strings,
  so backtick-quoted names (including spaces) are extracted as single
  tokens with the quoting stripped, alongside plain word-boundary
  identifiers.
"""

from __future__ import annotations

import re
from collections.abc import Iterable

_BACKTICKED = re.compile(r"`((?:[^`]|``)+)`")
_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def _expression_tokens(expression: str) -> set[str]:
    """Case-folded identifier tokens: backtick-quoted spans + bare words."""
    tokens: set[str] = set()
    remainder = expression
    for match in _BACKTICKED.finditer(expression):
        tokens.add(match.group(1).replace("``", "`").casefold())
    remainder = _BACKTICKED.sub(" ", remainder)
    for match in _IDENTIFIER.finditer(remainder):
        tokens.add(match.group(0).casefold())
    return tokens


def references_any_column(expression: str, columns: Iterable[str]) -> bool:
    """Whether a user expression may reference any of the named columns.

    Conservative by construction: a column name appearing as a bare
    identifier OR a backtick-quoted span counts as a reference, even if
    it is actually a function name or string literal — such collisions
    only cost the batch optimization, never correctness.
    """
    tokens = _expression_tokens(expression)
    return any(str(c).casefold() in tokens for c in columns)
