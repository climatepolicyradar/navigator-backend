"""
Functions to support the documents endpoints

old functions (non DFC) are moved to the deprecated_documents.py file.
"""

from functools import lru_cache


@lru_cache()
def get_query_template(filepath: str) -> str:
    """Read query for non-deleted docs and their associated data."""
    with open(filepath, "r") as file:
        return file.read()
