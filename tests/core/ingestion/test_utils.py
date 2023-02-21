from unittest.mock import MagicMock
from app.core.ingestion.utils import get_or_create
from app.data_migrations import populate_geography
from app.db.models.law_policy.family import Family, FamilyCategory, FamilyStatus, Slug
from sqlalchemy.orm import Session


"""
Note from author:

The model of "Family" was chosen as its a little more complicated
than the other models. It includes a "relationship" with Slug,
which in turns requires that the Family has at least one Slug.
"""


def add_a_family(db: Session):
    family = Family(
        import_id="1",
        title="title",
        geography_id=2,
        category_name="EXECUTIVE",
        description="description",
        family_status="Published",
    )
    db.add(family)
    add_a_slug_for_family1_and_flush(db)
    return family


def add_a_slug_for_family1_and_flush(db):
    slug = Slug(
        name="title_adb4",
        family_import_id="1",
        family_document_import_id=None,
    )
    db.add(slug)
    db.flush()
    # NOTE: Creating the Slug is part of test init,
    #      as we need a Slug to query for the Family.
    slug = db.query(Slug).one()
    assert slug


def test_get_or_create__gets(test_db: Session):
    populate_geography(test_db)
    test_db.flush()
    existing_family = add_a_family(test_db)

    family = get_or_create(
        test_db,
        Family,
        import_id="1",
    )
    assert family
    assert family == existing_family
    assert family.title == "title"
    assert family.description == "description"
    assert family.geography_id == 2
    assert family.category_name == "EXECUTIVE"
    assert family.family_status == "Published"
    assert FamilyCategory(family.category_name) == FamilyCategory.EXECUTIVE
    assert FamilyStatus(family.family_status) == FamilyStatus.PUBLISHED


def test_get_or_create__creates(test_db):
    populate_geography(test_db)
    new_family = get_or_create(
        test_db,
        Family,
        import_id="1",
        extra={
            "title": "title",
            "geography_id": 2,
            "category_name": "EXECUTIVE",
            "description": "description",
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
    assert family.category_name == "EXECUTIVE"
    assert family.family_status == "Published"
    assert FamilyCategory(family.category_name) == FamilyCategory.EXECUTIVE
    assert FamilyStatus(family.family_status) == FamilyStatus.PUBLISHED


def test_get_or_create__after_create(test_db):
    populate_geography(test_db)
    after_create = MagicMock()
    new_family = get_or_create(
        test_db,
        Family,
        import_id="1",
        extra={
            "title": "title",
            "geography_id": 2,
            "category_name": "EXECUTIVE",
            "description": "description",
            "family_status": "Published",
        },
        after_create=after_create,
    )
    add_a_slug_for_family1_and_flush(test_db)

    family = test_db.query(Family).one()
    assert family
    assert family == new_family
    after_create.assert_called_once_with(new_family)
