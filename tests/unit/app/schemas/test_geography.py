import pytest

from app.api.api_v1.schemas.geography import GeographyStatsDTO

FAMILY_CATEGORY_GEO_COUNTS = [
    {"Executive": 0, "Legislative": 0, "UNFCCC": 0},
    {"Executive": 0, "Legislative": 0, "UNFCCC": 0},
    {"Executive": 0, "Legislative": 0, "UNFCCC": 0},
]
ISO_CODES = ["TES", "XXX", "YYY"]


@pytest.mark.parametrize("family_category_geo_counts", FAMILY_CATEGORY_GEO_COUNTS)
@pytest.mark.parametrize("iso_code", ISO_CODES)
def test_family_category_counts_per_geo(family_category_geo_counts, iso_code):
    document_response = GeographyStatsDTO(
        display_name="Test",
        iso_code=iso_code,
        slug="test",
        family_counts=family_category_geo_counts,
    )
    assert document_response.iso_code == iso_code
    assert document_response.family_counts == family_category_geo_counts
