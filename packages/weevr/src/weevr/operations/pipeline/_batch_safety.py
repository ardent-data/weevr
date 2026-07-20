"""Sibling-reference scan for batchable per-column expressions.

Batched ``withColumns`` evaluates every expression against the pre-batch
frame; sequential ``withColumn`` lets expression N read output N-1. A
user-supplied expression fragment that names another column in the same
loop therefore changes meaning under batching. This scan decides, per
loop, whether batching is provably safe.

The invariant is directional: a false positive merely costs the
optimization (the loop falls back to sequential); a false negative would
silently change semantics and must be impossible. Three rules follow:

- **Case-insensitive comparison** — Spark resolves unquoted identifiers
  case-insensitively by default, so ``concat({col}, Backup_Col)``
  references ``backup_col``. Both sides are case-folded.
- **Quote-aware tokenization** — a character scanner tracks single-quote
  and double-quote string literals (with ``''``/``""`` doubling and
  backslash escapes) so a stray backtick INSIDE a string can never pair
  with a later backtick and swallow a real reference between them.
  Backtick-quoted identifier spans (with `````` doubling) are extracted
  as single tokens with the quoting stripped; bare identifiers tokenize
  on word boundaries. Unquoted identifiers are ASCII-only in Spark's
  grammar, so any non-ASCII column reference must use backticks — the
  backtick pass covers it.
- **Malformed input is a reference** — an expression the scanner cannot
  tokenize confidently (unterminated quote of any kind) is treated as
  referencing everything: sequential fallback, never a guess.
"""

from __future__ import annotations

import re
from collections.abc import Iterable

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def _expression_tokens(expression: str) -> set[str] | None:
    """Case-folded identifier tokens, or None when tokenization is unsafe.

    Linear scan with explicit quote states — regex pairing cannot tell an
    identifier-quote backtick from a backtick that merely appears inside
    a string literal, and that confusion is exactly the false-negative
    hole this scanner closes.
    """
    tokens: set[str] = set()
    plain: list[str] = []
    i = 0
    n = len(expression)
    while i < n:
        ch = expression[i]
        if ch in ("'", '"'):
            quote = ch
            i += 1
            while i < n:
                if expression[i] == "\\":
                    i += 2
                    continue
                if expression[i] == quote:
                    if i + 1 < n and expression[i + 1] == quote:  # doubled quote
                        i += 2
                        continue
                    i += 1
                    break
                i += 1
            else:
                return None  # unterminated literal — unsafe to reason about
            continue
        if ch == "`":
            i += 1
            span: list[str] = []
            while i < n:
                if expression[i] == "`":
                    if i + 1 < n and expression[i + 1] == "`":  # escaped backtick
                        span.append("`")
                        i += 2
                        continue
                    i += 1
                    break
                span.append(expression[i])
                i += 1
            else:
                return None  # unterminated identifier quote
            tokens.add("".join(span).casefold())
            continue
        plain.append(ch)
        i += 1
    for match in _IDENTIFIER.finditer("".join(plain)):
        tokens.add(match.group(0).casefold())
    return tokens


def references_any_column(expression: str, columns: Iterable[str]) -> bool:
    """Whether a user expression may reference any of the named columns.

    Conservative by construction: a column name appearing as a bare
    identifier OR a backtick-quoted span counts as a reference (even if
    it is actually a function name), and an expression the scanner cannot
    tokenize counts as referencing everything — such collisions only cost
    the batch optimization, never correctness.
    """
    tokens = _expression_tokens(expression)
    if tokens is None:
        return True
    return any(str(c).casefold() in tokens for c in columns)
