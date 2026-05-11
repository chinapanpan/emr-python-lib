"""Date and time utility functions."""

from datetime import datetime, timezone


def get_current_timestamp() -> str:
    """Get current UTC timestamp as string."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def format_date(dt: datetime, fmt: str = "%Y-%m-%d") -> str:
    """Format a datetime object to string."""
    return dt.strftime(fmt)


def parse_date(date_str: str, fmt: str = "%Y-%m-%d") -> datetime:
    """Parse a date string to datetime object."""
    return datetime.strptime(date_str, fmt)
