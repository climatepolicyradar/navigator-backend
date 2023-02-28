from unittest.mock import MagicMock

import pytest
from app.core.ingestion.utils import (
    Result,
    ResultType,
    get_or_create,
    get_result_counts,
    to_dict,
)
from app.data_migrations import populate_geography
from app.db.models.law_policy.family import Family, FamilyCategory, FamilyStatus
from sqlalchemy.orm import Session

from tests.core.ingestion.helpers import (
    FAMILY_IMPORT_ID,
    add_a_slug_for_family1_and_flush,
)


"""
Note from author:

The model of "Family" was chosen as its a little more complicated
than the other models. It includes a "relationship" with Slug,
which in turns requires that the Family has at least one Slug.
"""


def add_a_family(db: Session, description: str = "description"):
    family = Family(
        import_id=FAMILY_IMPORT_ID,
        title="title",
        geography_id=2,
        description=description,
        family_category="EXECUTIVE",
        family_status="Published",
    )
    db.add(family)
    add_a_slug_for_family1_and_flush(db)
    return family


def test_get_or_create__gets(test_db: Session):
    populate_geography(test_db)
    existing_family = add_a_family(test_db)

    family = get_or_create(
        test_db,
        Family,
        import_id=FAMILY_IMPORT_ID,
    )
    assert family
    assert family == existing_family
    assert family.title == "title"
    assert family.description == "description"
    assert family.geography_id == 2
    assert family.family_category == "EXECUTIVE"
    assert family.family_status == "Published"
    assert FamilyCategory(family.family_category) == FamilyCategory.EXECUTIVE
    assert FamilyStatus(family.family_status) == FamilyStatus.PUBLISHED


def test_get_or_create__creates(test_db):
    populate_geography(test_db)
    new_family = get_or_create(
        test_db,
        Family,
        import_id=FAMILY_IMPORT_ID,
        extra={
            "title": "title",
            "geography_id": 2,
            "description": "description",
            "family_category": "EXECUTIVE",
            "family_status": "Published",
        },
    )
    add_a_slug_for_family1_and_flush(test_db)

    family = test_db.query(Family).one()
    assert family
    assert family == new_family
    assert family.title == "title"
    assert family.description == "description"
    assert family.geography_id == 2
    assert family.family_category == "EXECUTIVE"
    assert family.family_status == "Published"
    assert FamilyCategory(family.family_category) == FamilyCategory.EXECUTIVE
    assert FamilyStatus(family.family_status) == FamilyStatus.PUBLISHED


def test_get_or_create__after_create(test_db):
    populate_geography(test_db)
    after_create = MagicMock()
    new_family = get_or_create(
        test_db,
        Family,
        import_id=FAMILY_IMPORT_ID,
        extra={
            "title": "title",
            "geography_id": 2,
            "description": "description",
            "family_category": "EXECUTIVE",
            "family_status": "Published",
        },
        after_create=after_create,
    )
    add_a_slug_for_family1_and_flush(test_db)

    family = test_db.query(Family).one()
    assert family
    assert family == new_family
    after_create.assert_called_once_with(new_family)


def test_to_dict(test_db):
    populate_geography(test_db)
    existing_family = add_a_family(
        test_db,
        description="""This is a really long description
        which should get truncated to 80 chars, the test
        will fail if it does not.
        """,
    )

    new_dict = to_dict(existing_family)

    assert new_dict == {
        "__class__": "Family",
        "description": "This is a really long description\n        which should get truncated to 80 chars...",
        "family_category": "EXECUTIVE",
        "family_status": "Published",
        "geography_id": "2",
        "import_id": FAMILY_IMPORT_ID,
        "title": "title",
    }


OK = Result(ResultType.OK)
ERROR = Result(ResultType.ERROR)
RESOLVED = Result(ResultType.RESOLVED)


@pytest.mark.parametrize(
    "result,expected_rows,expected_fails,expected_resolved",
    [
        ([], 0, 0, 0),
        ([OK, OK, OK], 3, 0, 0),
        ([ERROR, ERROR, ERROR], 3, 3, 0),
        ([RESOLVED, RESOLVED, RESOLVED], 3, 0, 3),
        ([OK, ERROR, RESOLVED], 3, 1, 1),
    ],
)
def test_result_counts(result, expected_rows, expected_fails, expected_resolved):
    rows, fails, resolved = get_result_counts(result)

    assert rows == expected_rows
    assert fails == expected_fails
    assert resolved == expected_resolved
