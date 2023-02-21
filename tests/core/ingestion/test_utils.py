from app.core.ingestion.utils import get_or_create
from app.data_migrations import populate_geography
from app.db.models.law_policy.family import Family
from sqlalchemy.orm import Session


def add_a_family(db: Session):
    family = Family(
        import_id="1",
        title="title",
        geography_id=2,
        category_name="EXECUTIVE",
        description="description",
        family_status="PUBLISHED",
    )
    db.add(family)
    db.commit()


def test_get_or_create__gets(test_db: Session):
    populate_geography(test_db)
    test_db.commit()
    add_a_family(test_db)
    test_db.commit()

    # TODO: Remove the following check
    assert test_db.query(Family).one()

    family = get_or_create(
        test_db,
        Family,
        import_id="1",
    )
    assert family
    assert family.title == "title"
    assert family.description == "description"
    assert family.geography_id == 2
    assert family.category_name == "EXECUTIVE"
    assert family.family_status == "PUBLISHED"


def test_get_or_create__extra_args(test_db):
    populate_geography(test_db)
    family = get_or_create(
        test_db,
        Family,
        import_id="1",
        extra={
            "title": "title",
            "geography_id": 2,
            "category_name": "EXECUTIVE",
            "description": "description",
            "family_status": "PUBLISHED",
        },
    )
    assert family
    assert family.title == "title"
    assert family.description == "description"
    assert family.geography_id == 2
    assert family.category_name == "EXECUTIVE"
    assert family.family_status == "PUBLISHED"
