from app.extract.navigator_family import NavigatorFamily
from app.models import ExtractedEnvelope, Identified


def identify_navigator_family(
    extracted: ExtractedEnvelope[NavigatorFamily],
) -> Identified[NavigatorFamily]:
    return Identified(
        data=extracted.data,
        source=extracted.source_name,
        id=extracted.data.import_id,
    )
