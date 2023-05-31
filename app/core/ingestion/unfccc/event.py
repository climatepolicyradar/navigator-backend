from sqlalchemy.orm import Session
from app.core.ingestion.unfccc.ingest_row_unfccc import UNFCCCDocumentIngestRow
from app.db.models.law_policy.family import EventStatus, FamilyEvent


TYPE_MAP = {
    "ANNEX": "Updated",
    "SUPPORTING DOCUMENTATION": "Updated",
    "MAIN": "Passed/Approved",
}


def _get_type_from_role(role):
    if role in TYPE_MAP:
        return TYPE_MAP[role]
    return "Other"


def _create_event_id(doc_id: str) -> str:
    id_bits = doc_id.split(".")
    if len(id_bits) < 4:
        raise ValueError(f"Document import id has unexpected format {doc_id}")
    id_bits[1] = "unfccc_event"
    return ".".join(id_bits)


def create_event_from_row(db: Session, row: UNFCCCDocumentIngestRow):
    # Don't create an event for summaries
    if row.document_role.upper() == "SUMMARY":
        return

    event_type = _get_type_from_role(row.document_role.upper())
    event = FamilyEvent(
        import_id=_create_event_id(row.cpr_document_id),
        title=event_type,
        date=row.date,
        event_type_name=event_type,
        family_import_id=row.cpr_family_id,
        family_document_import_id=row.cpr_document_id,
        status=EventStatus.OK,
    )

    db.add(event)
    db.flush()
