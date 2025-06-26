"""Helper functions for the repository layer."""

from functools import lru_cache


@lru_cache()
def get_query_template(filepath: str) -> str:
    """Read query for non-deleted docs and their associated data."""
    with open(filepath, "r") as file:
        return file.read()
