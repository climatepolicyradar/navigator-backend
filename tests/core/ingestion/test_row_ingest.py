from pydantic.dataclasses import dataclass
from pydantic import ConfigDict, Extra
import pytest
from app.core.ingestion.ingest_row_base import BaseIngestRow, validate_csv_columns


@dataclass(config=ConfigDict(frozen=True, validate_assignment=True, extra=Extra.forbid))
class BaseIngestRowTest(BaseIngestRow):
    """A class for testing"""

    col_a: str
    col_b: str

    VALID_COLUMNS = set(["Col A", "Col B"])


@pytest.mark.parametrize(
    "input_columns",
    [
        ["col a", "col b", "col c"],
        ["cOl a", "col b", "col c"],
        ["col a", "Col b", "col C"],
    ],
)
def test_validate_column_names_caseinsensitive(input_columns):
    valid_column_names = set(["col a", "col b", "col c"])

    missing_columns = validate_csv_columns(input_columns, valid_column_names)
    assert not missing_columns


def test_base_from_row():
    test_data = {
        "COL A": "23",
        "coL B": "57",
    }
    row = BaseIngestRowTest.from_row(1, test_data)

    assert row.col_a == "23"
    assert row.col_b == "57"
