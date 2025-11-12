from app.extract.connectors import NavigatorFamily
from app.models import ExtractedEnvelope, Identified


def identify_navigator_family(
    extracted: list[ExtractedEnvelope[list[NavigatorFamily]]],
) -> Identified[NavigatorFamily]:
    """Identify a navigator family from extracted envelopes.

    TODO: This currently only processes the first envelope and first family.
    Should be refactored to handle multiple envelopes/families properly.
    See ticket: [TICKET-NUMBER]
    """
    if not extracted:
        raise ValueError("Cannot identify from empty extracted list")

    first_envelope = extracted[0]

    if not first_envelope.data:
        raise ValueError("First envelope contains no family data")

    first_family = first_envelope.data[0]

    return Identified(
        data=first_family,
        source=first_envelope.source_name,
        id=first_family.import_id,
    )
