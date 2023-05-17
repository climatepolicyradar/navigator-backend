from typing import Mapping, Sequence, Union

from pydantic.dataclasses import dataclass
from pydantic.config import ConfigDict, Extra

from app.core.ingestion.match import match_unknown_value


@dataclass(config=ConfigDict(validate_assignment=True, extra=Extra.forbid))
class TaxonomyEntry:
    """Details a single taxonomy field"""

    allow_blanks: bool
    allowed_values: Sequence[str]


Taxonomy = Mapping[str, TaxonomyEntry]
MetadataJson = Mapping[str, Union[str, Sequence[str]]]


def resolve_unknown(unknown_set: set[str], allowed_set: set[str]) -> set[str]:
    suggestions = set()
    for unknown_value in unknown_set:
        suggestion = match_unknown_value(unknown_value, allowed_set)
        if suggestion:
            suggestions.add(suggestion)
    return suggestions
