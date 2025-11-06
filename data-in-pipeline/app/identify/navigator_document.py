from app.extract.navigator_document import NavigatorDocument
from app.models import ExtractedEnvelope, Identified


def identify_navigator_document(
    extracted: ExtractedEnvelope[NavigatorDocument],
) -> Identified[NavigatorDocument]:
    return Identified(
        data=extracted.data,
        source=extracted.source_name,
        id=extracted.data.import_id,
    )
