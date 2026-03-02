"""Duration string parser for time-based thresholds."""

import re
from datetime import timedelta

_DURATION_PATTERN = re.compile(r"^(?:(\d+)d)?(?:(\d+)h)?(?:(\d+)m)?$")


def parse_duration(value: str) -> timedelta:
    """Parse a duration string into a timedelta.

    Supports day (d), hour (h), and minute (m) units. Units can be
    combined in descending order: ``2h30m``, ``1d12h``, ``1d6h30m``.

    Args:
        value: Duration string (e.g. ``"24h"``, ``"30m"``, ``"7d"``, ``"2h30m"``).

    Returns:
        Parsed timedelta.

    Raises:
        ValueError: If the string is empty, contains unknown units,
            or does not match the expected format.
    """
    if not value or not value.strip():
        msg = f"Duration string must not be empty: {value!r}"
        raise ValueError(msg)

    stripped = value.strip()
    match = _DURATION_PATTERN.match(stripped)
    if not match:
        msg = (
            f"Invalid duration format: {value!r}. "
            "Expected combinations of <number>d, <number>h, <number>m "
            "(e.g. '24h', '2h30m', '7d')."
        )
        raise ValueError(msg)

    days_str, hours_str, minutes_str = match.groups()
    if days_str is None and hours_str is None and minutes_str is None:
        msg = f"Duration string must contain at least one unit: {value!r}"
        raise ValueError(msg)

    days = int(days_str) if days_str else 0
    hours = int(hours_str) if hours_str else 0
    minutes = int(minutes_str) if minutes_str else 0

    return timedelta(days=days, hours=hours, minutes=minutes)
