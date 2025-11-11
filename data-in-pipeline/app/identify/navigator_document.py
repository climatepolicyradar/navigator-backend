from app.extract.connectors import NavigatorDocument
from app.logging_config import ensure_logging_active
from app.models import ExtractedEnvelope, Identified

ensure_logging_active(force_instrumentation=True)


def identify_navigator_document(
    extracted: ExtractedEnvelope[NavigatorDocument],
) -> Identified[NavigatorDocument]:
    return Identified(
        data=extracted.data,
        source=extracted.source_name,
        id=extracted.data.import_id,
    )
