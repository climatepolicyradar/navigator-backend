from datetime import datetime
from app.core.ingestion.unfccc.ingest_row_unfccc import UNFCCCDocumentIngestRow
from app.core.ingestion.utils import ResultType, UNFCCCIngestContext
from app.core.ingestion.validator import (
    validate_unfccc_document_row,
)
from app.core.organisation import get_organisation_taxonomy

from tests.core.ingestion.helpers import (
    populate_for_ingest,
)


def test_validate_row__multiple_collection_ids(test_db):
    context = UNFCCCIngestContext()
    populate_for_ingest(test_db)
    _, taxonomy = get_organisation_taxonomy(test_db, context.org_id)

    row = UNFCCCDocumentIngestRow(
        row_number=1,
        category="UNFCCC",
        submission_type="Plan",
        family_name="family_name",
        document_title="document_title",
        documents="documents",
        author="author",
        author_type="Party",
        geography="GBR",
        geography_iso="GBR",
        date=datetime.now(),
        document_role="MAIN",
        document_variant="Original Language",
        language=["en"],
        cpr_collection_id=["id1", "id2"],
        cpr_document_id="cpr_document_id",
        cpr_family_id="cpr_family_id",
        cpr_family_slug="cpr_family_slug",
        cpr_document_slug="cpr_document_slug",
        cpr_document_status="PUBLISHED",
        download_url="download_url",
    )

    validate_unfccc_document_row(test_db, context=context, row=row, taxonomy=taxonomy)

    assert context.results
    assert len(context.results) == 1
    assert context.results[0].type == ResultType.OK
