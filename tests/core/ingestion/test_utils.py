from app.core.ingestion.utils import get_or_create
from app.data_migrations import populate_geography
from app.db.models.law_policy.family import Family, FamilyCategory, FamilyStatus, Slug
from sqlalchemy.orm import Session


def add_a_family(db: Session):
    family = Family(
        import_id="1",
        title="title",
        geography_id=2,
        category_name=FamilyCategory.EXECUTIVE,
        description="description",
        family_status=FamilyStatus.PUBLISHED,
    )
    slug = Slug(
        name="title_adb4",
        family_import_id="1",
        family_document_import_id=None,
    )
    db.add(family)
    db.add(slug)
    db.flush()
    return family


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
    assert family.category_name == FamilyCategory.EXECUTIVE
    assert family.family_status == FamilyStatus.PUBLISHED
    # TODO: These fail
    # assert family.category_name == "EXECUTIVE"
    # assert family.family_status == "PUBLISHED"


def test_get_or_create__extra_args(test_db):
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
            "family_status": "PUBLISHED",
        },
    )
    new_slug = get_or_create(
        test_db,
        Slug,
        name="title_23a1",
        extra={
            "family_import_id": "1",
            "family_document_import_id": None,
        },
    )

    family = test_db.query(Family).one()
    slug = test_db.query(Slug).one()

    assert slug == new_slug
    assert family
    assert family == new_family
    assert family.title == "title"
    assert family.description == "description"
    assert family.geography_id == 2
    assert family.category_name == "EXECUTIVE"
    assert family.family_status == "PUBLISHED"
    # TODO: These fail
    # assert family.category_name == FamilyCategory.EXECUTIVE
    # assert family.family_status == FamilyStatus.PUBLISHED
