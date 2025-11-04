from app.extract.navigator_family import NavigatorFamily
from app.models import Extracted, Identified


def identify_navigator_family(
    extracted_document: Extracted[NavigatorFamily],
) -> Identified[NavigatorFamily]:
    return Identified(data=extracted_document, id=extracted_document.data.import_id)
