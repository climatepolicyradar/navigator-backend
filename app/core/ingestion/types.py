from dataclasses import dataclass
import enum
from typing import Mapping, Sequence

from pydantic.dataclasses import dataclass as pydantic_dataclass
from pydantic.config import ConfigDict


@pydantic_dataclass(config=ConfigDict(validate_assignment=True, extra="forbid"))
class TaxonomyEntry:
    """Details a single taxonomy field"""

    allow_blanks: bool
    allowed_values: Sequence[str]
    allow_any: bool = False


Taxonomy = Mapping[str, TaxonomyEntry]


class ResultType(str, enum.Enum):
    """Result type used when processing metadata values."""

    OK = "Ok"
    RESOLVED = "Resolved"
    ERROR = "Error"


@dataclass
class Result:
    """Augmented result class for reporting extra details about processed metadata."""

    type: ResultType = ResultType.OK
    details: str = ""
