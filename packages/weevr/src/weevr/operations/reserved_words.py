"""Back-compat re-export.

Canonical location is :mod:`weevr.reserved_words`. This shim preserves the
``weevr.operations.reserved_words`` import path for engine-side consumers
(notably :mod:`weevr.operations.naming` and the function-local import inside
:meth:`weevr.model.column_set.ReservedWordConfig._validate_rename_strategy`).
"""

from weevr.reserved_words import *  # noqa: F401,F403
from weevr.reserved_words import __all__  # noqa: F401
