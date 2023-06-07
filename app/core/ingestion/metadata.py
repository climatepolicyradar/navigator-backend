from typing import Any, Mapping, Sequence, Union, Optional

from pydantic.dataclasses import dataclass
from pydantic.config import ConfigDict, Extra

from app.core.ingestion.match import match_unknown_value
from app.core.ingestion.utils import Result, ResultType


@dataclass(config=ConfigDict(validate_assignment=True, extra=Extra.forbid))
class TaxonomyEntry:
    """Details a single taxonomy field"""

    allow_blanks: bool
    allowed_values: Sequence[str]
    allow_any: Optional[bool] = False


Taxonomy = Mapping[str, TaxonomyEntry]
MetadataJson = Mapping[str, Union[str, Sequence[str]]]


def resolve_unknown(unknown_set: set[str], allowed_set: set[str]) -> set[str]:
    suggestions = set()
    for unknown_value in unknown_set:
        suggestion = match_unknown_value(unknown_value, allowed_set)
        if suggestion:
            suggestions.add(suggestion)
    return suggestions


def build_metadata_field(
    row_number: int, taxonomy: Taxonomy, ingest_values: Any, tax_key: str
) -> tuple[Result, list[str]]:
    if type(ingest_values) == str:
        ingest_values = [ingest_values]
    row_set = set(ingest_values)
    allowed_set: set[str] = set(taxonomy[tax_key].allowed_values)
    allow_blanks = taxonomy[tax_key].allow_blanks
    allow_any = taxonomy[tax_key].allow_any

    if len(row_set) == 0:
        if not allow_blanks:
            details = f"Row {row_number} is blank for {tax_key} - which is not allowed."
            return Result(type=ResultType.ERROR, details=details), []
        return Result(), []  # field is blank and allowed

    if allow_any:
        return Result(), ingest_values

    unknown_set = row_set.difference(allowed_set)
    if not unknown_set:
        return Result(), ingest_values  # all is well - everything found

    resolved_set = resolve_unknown(unknown_set, allowed_set)

    if len(resolved_set) == len(unknown_set):
        details = f"Row {row_number} RESOLVED: {resolved_set}"
        vals = row_set.difference(unknown_set).union(resolved_set)
        return Result(type=ResultType.RESOLVED, details=details), list(vals)

    # If we get here we have not managed to resolve the unknown values.

    details = (
        f"Row {row_number} has value(s) for '{tax_key}' that is/are "
        f"unrecognised: '{unknown_set}' "
    )

    if len(resolved_set):
        details += f"able to resolve: {resolved_set}"

    return Result(type=ResultType.ERROR, details=details), []
