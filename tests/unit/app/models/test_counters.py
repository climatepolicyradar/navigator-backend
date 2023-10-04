from app.data_migrations import populate_counters
from app.db.models.app.counters import CountedEntity, EntityCounter


def test_import_id_generation(test_db):
    populate_counters(test_db)
    rows = test_db.query(EntityCounter).count()
    assert rows > 0

    row: EntityCounter = (
        test_db.query(EntityCounter).filter(EntityCounter.prefix == "CCLW").one()
    )
    assert row is not None

    assert row.prefix == "CCLW"
    assert row.counter == 0

    import_id = row.create_import_id(CountedEntity.Family)
    assert import_id == "CCLW.family.i00000001.n0000"

    row: EntityCounter = (
        test_db.query(EntityCounter).filter(EntityCounter.prefix == "CCLW").one()
    )
    assert row.counter == 1
