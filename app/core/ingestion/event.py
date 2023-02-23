from typing import Any

from sqlalchemy.orm import Session
from app.core.ingestion.ingest_row import EventIngestRow
from app.core.ingestion.utils import get_or_create, to_dict

from app.db.models.law_policy import FamilyEvent


def family_event_from_row(
    db: Session,
    row: EventIngestRow,
    result: dict[str, Any],
) -> FamilyEvent:
    """
    Create any missing Family, FamilyDocument & Associated links from the given row

    :param [Session] db: connection to the database.
    :param [EventIngestRow] row: the row built from the events CSV.
    :param [dict[str, Any]] result: a result dict in which to track what was created
    :raises [ValueError]: When there is an existing family name that only differs by
        case or when the geography associated with this row cannot be found in the
        database.
    :return [FamilyEvent]: The family event that was either retrieved or created
    """
    # Get or create FamilyEvent
    family_event = _maybe_create_family_event(db, row, result)

    return family_event


def _maybe_create_family_event(
    db: Session, row: EventIngestRow, result: dict[str, Any]
) -> FamilyEvent:
    family_event = get_or_create(
        db,
        FamilyEvent,
        import_id=row.cpr_event_id,
        extra={
            "title": row.title,
            "date": row.date,
            "event_type_name": row.event_type,
            "family_import_id": row.cpr_family_id,
            "family_document_import_id": None,  # TODO: link to documents in future
            "status": row.event_status,
        },
    )
    family_event_results = result.get("family_events", [])
    family_event_results.append(to_dict(family_event))
    result["family_events"] = family_event_results
    return family_event
