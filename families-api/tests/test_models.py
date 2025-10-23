from datetime import datetime
from app.models import FamilyDocumentPublic, FamilyDocumentStatus, PhysicalDocument
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
