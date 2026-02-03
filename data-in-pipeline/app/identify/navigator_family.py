from app.extract.connectors import NavigatorFamily
from app.models import ExtractedEnvelope, Identified


def identify_navigator_family(
    extracted: list[ExtractedEnvelope[list[NavigatorFamily]]],
) -> Identified[NavigatorFamily]:
    """Identify a navigator family from extracted envelopes.

    TODO: This currently only processes the first envelope and first family.
    Should be refactored to handle multiple envelopes/families properly.
    See ticket: APP-1419
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


def _identify(
    envelope: ExtractedEnvelope[list[NavigatorFamily]],
    family: NavigatorFamily,
) -> Identified[NavigatorFamily]:
    return Identified(
        data=family,
        source=envelope.source_name,
        id=family.import_id,
    )


def identify_navigator_families(
    extracted: list[ExtractedEnvelope[list[NavigatorFamily]]],
) -> list[Identified[NavigatorFamily]]:
    """Identify all navigator families from extracted envelopes."""
    if not extracted:
        raise ValueError("Cannot identify from empty extracted list")

    identified = [
        _identify(envelope, family)
        for envelope in extracted
        if envelope.data
        for family in envelope.data
    ]

    if not identified:
        raise ValueError("No families could be identified from envelopes")

    return identified
