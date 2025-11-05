from app.extract.navigator_document import NavigatorDocument
from app.models import Extracted, Identified


def identify_navigator_document(
    extracted: Extracted[NavigatorDocument],
) -> Identified[NavigatorDocument]:
    return Identified(
        data=extracted.data,
        source=extracted.source,
        id=extracted.data.import_id,
    )
