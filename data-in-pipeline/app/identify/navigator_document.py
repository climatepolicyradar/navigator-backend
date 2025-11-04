from app.extract.navigator_document import NavigatorDocument
from app.models import Extracted, Identified


def identify_navigator_document(
    extracted_document: Extracted[NavigatorDocument],
) -> Identified[NavigatorDocument]:
    return Identified(data=extracted_document, id=extracted_document.data.import_id)
