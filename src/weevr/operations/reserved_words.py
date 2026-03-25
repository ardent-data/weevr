"""Reserved word presets for identifier validation.

Provides curated frozensets of reserved words for multiple query languages
and a resolution function that composes presets with user-defined extend
and exclude lists.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from weevr.model.column_set import ReservedWordConfig

_log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# ANSI SQL reserved words (~80 words, per SQL:2016)
# ---------------------------------------------------------------------------
ANSI_RESERVED_WORDS: frozenset[str] = frozenset(
    {
        # SELECT/FROM clauses
        "select",
        "from",
        "where",
        # INSERT/UPDATE/DELETE
        "insert",
        "update",
        "delete",
        "into",
        "values",
        "set",
        # CREATE/ALTER/DROP
        "create",
        "alter",
        "drop",
        "table",
        "index",
        "column",
        "add",
        # JOIN operations
        "join",
        "inner",
        "outer",
        "left",
        "right",
        "full",
        "cross",
        "natural",
        "on",
        "using",
        # GROUP/ORDER/HAVING
        "group",
        "order",
        "by",
        "having",
        "asc",
        "desc",
        # Logical operators
        "and",
        "or",
        "not",
        # Conditionals
        "case",
        "when",
        "then",
        "else",
        "end",
        # NULL/DISTINCT
        "null",
        "is",
        "distinct",
        # IN/BETWEEN/LIKE
        "in",
        "between",
        "like",
        "exists",
        # ALL/ANY/SOME
        "all",
        "any",
        "some",
        # UNION/EXCEPT/INTERSECT
        "union",
        "except",
        "intersect",
        # WITH (CTEs)
        "with",
        "recursive",
        # LIMIT/OFFSET/FETCH
        "limit",
        "offset",
        "fetch",
        # Constraints
        "constraint",
        "primary",
        "foreign",
        "key",
        "references",
        "check",
        "default",
        # Authorization
        "grant",
        "revoke",
        # Boolean/Date/Time
        "true",
        "false",
        "current_date",
        "current_time",
        "current_timestamp",
        "current_user",
        # Transaction control
        "begin",
        "commit",
        "rollback",
        # Cursors/Declarations
        "declare",
        "cursor",
        "open",
        "close",
        "for",
        # Aliases
        "as",
    }
)

# ---------------------------------------------------------------------------
# DAX reserved words (~28 words)
# Words that conflict with DAX function names or keywords when used as
# column or table names in Power BI semantic models.
# ---------------------------------------------------------------------------
DAX_RESERVED_WORDS: frozenset[str] = frozenset(
    {
        "all",
        "allnoblankrow",
        "allselected",
        "blank",
        "calculate",
        "calculatetable",
        "currency",
        "date",
        "datetime",
        "divide",
        "duration",
        "error",
        "false",
        "filter",
        "integer",
        "measure",
        "null",
        "percentage",
        "real",
        "string",
        "switch",
        "table",
        "text",
        "time",
        "true",
        "value",
        "values",
        "var",
    }
)

# ---------------------------------------------------------------------------
# M language (Power Query) reserved words (~21 words)
# Keywords that require #"quoting" in M expressions when used as identifiers.
# ---------------------------------------------------------------------------
M_RESERVED_WORDS: frozenset[str] = frozenset(
    {
        "and",
        "as",
        "each",
        "else",
        "error",
        "false",
        "if",
        "in",
        "is",
        "let",
        "meta",
        "not",
        "null",
        "or",
        "otherwise",
        "section",
        "shared",
        "then",
        "true",
        "try",
        "type",
    }
)

# ---------------------------------------------------------------------------
# T-SQL reserved words (~156 words, self-contained)
# Full set from Microsoft's official Transact-SQL reserved keywords reference.
# Includes ANSI overlaps so preset: [tsql] alone gives complete protection.
# ---------------------------------------------------------------------------
TSQL_RESERVED_WORDS: frozenset[str] = frozenset(
    {
        "add",
        "all",
        "alter",
        "and",
        "any",
        "as",
        "asc",
        "authorization",
        "backup",
        "begin",
        "between",
        "break",
        "browse",
        "bulk",
        "by",
        "cascade",
        "case",
        "check",
        "checkpoint",
        "close",
        "clustered",
        "coalesce",
        "collate",
        "column",
        "commit",
        "compute",
        "constraint",
        "contains",
        "containstable",
        "continue",
        "convert",
        "create",
        "cross",
        "current",
        "current_date",
        "current_time",
        "current_timestamp",
        "current_user",
        "cursor",
        "database",
        "dbcc",
        "deallocate",
        "declare",
        "default",
        "delete",
        "deny",
        "desc",
        "disk",
        "distinct",
        "distributed",
        "double",
        "drop",
        "dump",
        "else",
        "end",
        "errlvl",
        "escape",
        "except",
        "exec",
        "execute",
        "exists",
        "exit",
        "external",
        "fetch",
        "file",
        "fillfactor",
        "for",
        "foreign",
        "freetext",
        "freetexttable",
        "from",
        "full",
        "function",
        "goto",
        "grant",
        "group",
        "having",
        "holdlock",
        "identity",
        "identity_insert",
        "identitycol",
        "if",
        "in",
        "index",
        "inner",
        "insert",
        "intersect",
        "into",
        "is",
        "join",
        "key",
        "kill",
        "left",
        "like",
        "lineno",
        "load",
        "merge",
        "national",
        "nocheck",
        "nonclustered",
        "not",
        "null",
        "nullif",
        "of",
        "off",
        "offsets",
        "on",
        "open",
        "opendatasource",
        "openquery",
        "openrowset",
        "openxml",
        "option",
        "or",
        "order",
        "outer",
        "over",
        "percent",
        "pivot",
        "plan",
        "precision",
        "primary",
        "print",
        "proc",
        "procedure",
        "public",
        "raiserror",
        "read",
        "readtext",
        "reconfigure",
        "references",
        "replication",
        "restore",
        "restrict",
        "return",
        "revert",
        "revoke",
        "right",
        "rollback",
        "rowcount",
        "rowguidcol",
        "rule",
        "save",
        "schema",
        "securityaudit",
        "select",
        "semantickeyphrasetable",
        "semanticsimilaritydetailstable",
        "semanticsimilaritytable",
        "session_user",
        "set",
        "setuser",
        "shutdown",
        "some",
        "statistics",
        "system_user",
        "table",
        "tablesample",
        "textsize",
        "then",
        "to",
        "top",
        "tran",
        "transaction",
        "trigger",
        "truncate",
        "try_convert",
        "tsequal",
        "union",
        "unique",
        "unpivot",
        "update",
        "updatetext",
        "use",
        "user",
        "values",
        "varying",
        "view",
        "waitfor",
        "when",
        "where",
        "while",
        "with",
        "writetext",
    }
)

# ---------------------------------------------------------------------------
# Preset registry
# ---------------------------------------------------------------------------
PRESET_REGISTRY: dict[str, frozenset[str]] = {
    "ansi": ANSI_RESERVED_WORDS,
    "dax": DAX_RESERVED_WORDS,
    "m": M_RESERVED_WORDS,
    "powerbi": DAX_RESERVED_WORDS | M_RESERVED_WORDS,
    "tsql": TSQL_RESERVED_WORDS,
}


def resolve_effective_words(config: ReservedWordConfig) -> frozenset[str]:
    """Resolve the effective reserved word set from config.

    When ``preset`` is ``None`` (omitted), returns the ANSI SQL word set
    as the default. When ``preset`` is specified, returns the union of all
    listed preset word sets.  The ``extend`` and ``exclude`` lists compose
    on top of the resolved base.

    Args:
        config: Reserved word configuration with optional preset, extend,
            and exclude fields.

    Returns:
        The effective set of reserved words to check against.
    """
    if config.preset is None:
        base = ANSI_RESERVED_WORDS
    elif len(config.preset) == 0:
        base = frozenset()
    else:
        base = frozenset().union(*(PRESET_REGISTRY[p.value] for p in config.preset))

    effective = (base | {w.lower() for w in config.extend}) - {w.lower() for w in config.exclude}

    _log.debug(
        "Reserved word protection: %d effective words (preset=%s, +%d extend, -%d exclude)",
        len(effective),
        [p.value for p in config.preset] if config.preset is not None else "default",
        len(config.extend),
        len(config.exclude),
    )

    return effective
