from datetime import datetime

from app.models import (
    CorpusPublic,
    CorpusTypePublic,
    FamilyDocumentPublic,
    FamilyDocumentStatus,
    FamilyEvent,
    FamilyPublic,
    Organisation,
    PhysicalDocument,
)
from app.settings import settings


def test_FamilyDocumentPublic_cdn_object_computed_field_prepends_cdn_url_and_navigator():

    family = FamilyDocumentPublic(
        import_id="123",
        document_status=FamilyDocumentStatus.CREATED,
        valid_metadata={},
        physical_document=PhysicalDocument(
            id=123,
            title="Test Document",
            md5_sum="abc123",
            source_url="http://example.com/doc.pdf",
            content_type="application/pdf",
            cdn_object="abc/123/doc.pdf",
        ),
        variant_name=None,
        unparsed_slug=[],
        unparsed_events=[],
        last_modified=datetime.now(),
    )

    assert family.cdn_object.startswith(settings.cdn_url)
    assert family.cdn_object == f"{settings.cdn_url}/navigator/abc/123/doc.pdf"


def test_FamilyPublic_uses_date_from_event_with_datetime_event_name_for_published_date_when_present():
    datetime_event = FamilyEvent(
        import_id="1234",
        title="Test",
        date=datetime(2023, 1, 1, 0, 00),
        event_type_name="Passed/Approved",
        status="",
        valid_metadata={"datetime_event_name": ["Passed/Approved"]},
    )

    events = [
        FamilyEvent(
            import_id="1234",
            title="Test",
            date=datetime(2020, 1, 1, 0, 00),
            event_type_name="Cancelled",
            status="",
            valid_metadata={"datetime_event_name": ["Passed/Approved"]},
        ),
        datetime_event,
    ]

    family = FamilyPublic(
        import_id="Test",
        title="Test",
        concepts=[],
        corpus=CorpusPublic(
            import_id="Test",
            title="Test",
            corpus_type_name="Test",
            corpus_text="Test",
            attribution_url=None,
            corpus_image_url=None,
            organisation=Organisation(id=1, name="Test"),
            corpus_type=CorpusTypePublic(name="Test", description=""),
        ),
        unparsed_events=events,
    )

    assert family.published_date == datetime_event.date


def test_FamilyPublic_uses_date_from_earliest_event_for_published_date_when_datetime_event_not_present():
    events = [
        FamilyEvent(
            import_id="1234",
            title="Test",
            date=datetime(2023, 1, 1, 0, 00),
            event_type_name="Cancelled",
            status="",
            valid_metadata={"datetime_event_name": ["Passed/Approved"]},
        )
    ]

    family = FamilyPublic(
        import_id="Test",
        title="Test",
        concepts=[],
        corpus=CorpusPublic(
            import_id="Test",
            title="Test",
            corpus_type_name="Test",
            corpus_text="Test",
            attribution_url=None,
            corpus_image_url=None,
            organisation=Organisation(id=1, name="Test"),
            corpus_type=CorpusTypePublic(name="Test", description=""),
        ),
        unparsed_events=events,
    )

    assert family.published_date == events[0].date


def test_FamilyPublic_uses_date_from_earliest_datetime_event_for_published_date_when_multiple_present():
    earlier_event_date = datetime(2020, 1, 1, 0, 00)
    later_event_date = datetime(2023, 1, 1, 0, 00)

    events = [
        FamilyEvent(
            import_id="1234",
            title="Test",
            date=later_event_date,
            event_type_name="Passed/Approved",
            status="",
            valid_metadata={"datetime_event_name": ["Passed/Approved"]},
        ),
        FamilyEvent(
            import_id="1235",
            title="Test",
            date=earlier_event_date,
            event_type_name="Passed/Approved",
            status="",
            valid_metadata={"datetime_event_name": ["Passed/Approved"]},
        ),
    ]

    family = FamilyPublic(
        import_id="Test",
        title="Test",
        concepts=[],
        corpus=CorpusPublic(
            import_id="Test",
            title="Test",
            corpus_type_name="Test",
            corpus_text="Test",
            attribution_url=None,
            corpus_image_url=None,
            organisation=Organisation(id=1, name="Test"),
            corpus_type=CorpusTypePublic(name="Test", description=""),
        ),
        unparsed_events=events,
    )

    assert family.published_date == earlier_event_date
