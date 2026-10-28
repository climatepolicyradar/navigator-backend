"""Non-fatal transform warnings are returned from `transform`."""

import pytest

from app.models import Identified
from app.transform.models import UnknownGeography
from app.transform.navigator_family import transform
from tests.factories import (
    NavigatorCorpusFactory,
    NavigatorFamilyFactory,
)


def _transform_warnings(**family_overrides):
    defaults = dict(
        import_id="family",
        corpus=NavigatorCorpusFactory.build(import_id="CCLW.corpus.i00000001.n0000"),
        created="2020-01-01T00:00:00Z",
        geographies=["NOT-A-GEOGRAPHY"],
        documents=[],
        events=[],
        collections=[],
        concepts=[],
        metadata={},
        category="Legislative",
    )
    family = NavigatorFamilyFactory.build(**{**defaults, **family_overrides})
    return (
        transform(
            Identified(data=family, id=family.import_id, source="navigator_family")
        )
        .unwrap()
        .warnings
    )


def test_family_warnings_are_returned_for_a_family_without_documents():
    assert _transform_warnings() == [
        UnknownGeography(family_import_id="family", geography_id="NOT-A-GEOGRAPHY")
    ]


@pytest.mark.parametrize("geography_id", ["VAT", "NIU", "COK", "XKX"])
def test_manually_mapped_geographies(geography_id: str) -> None:
    assert _transform_warnings(geographies=[geography_id]) == []
