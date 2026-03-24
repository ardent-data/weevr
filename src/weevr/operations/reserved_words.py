"""ANSI SQL reserved words for identifier validation."""

# Frozenset of ~80 ANSI SQL reserved words per SQL:2016.
# Used to prevent conflicts between user-specified identifiers and SQL keywords.
# All entries are lowercase per SQL standard case-insensitivity conventions.
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
