from app.extract.connectors import NavigatorFamily
from app.logging_config import ensure_logging_active
from app.models import ExtractedEnvelope, Identified

ensure_logging_active()


def identify_navigator_family(
    extracted: ExtractedEnvelope[NavigatorFamily],
) -> Identified[NavigatorFamily]:
    return Identified(
        data=extracted.data,
        source=extracted.source_name,
        id=extracted.data.import_id,
    )
