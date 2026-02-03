from app.extract.connectors import NavigatorFamily
from app.models import ExtractedEnvelope, Identified


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
