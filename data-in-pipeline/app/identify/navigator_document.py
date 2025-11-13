from app.extract.connectors import NavigatorDocument
from app.logging_config import ensure_logging_active, get_logger
from app.models import ExtractedEnvelope, Identified

_LOGGER = get_logger()
ensure_logging_active()


def identify_navigator_document(
    extracted: ExtractedEnvelope[NavigatorDocument],
) -> Identified[NavigatorDocument]:
    return Identified(
        data=extracted.data,
        source=extracted.source_name,
        id=extracted.data.import_id,
    )
