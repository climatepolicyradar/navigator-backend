import pytest
from sqlalchemy.exc import NoResultFound
from sqlalchemy.orm import Session
from app.core.ingestion.ingest_row import IngestRow
from app.core.ingestion.metadata import build_metadata
from app.core.ingestion.organisation import get_organisation_taxonomy
from app.core.ingestion.utils import ResultType
from tests.core.ingestion.helpers import get_ingest_row_data, init_for_ingest

METADATA_KEYS = set(
    ["topic", "hazard", "sector", "keyword", "framework", "instrument", "document_type"]
)


def test_build_metadata__all_fields(test_db):
    init_for_ingest(test_db)
    _, taxonomy = get_organisation_taxonomy(test_db, org_id=1)
    row = IngestRow.from_row(1, get_ingest_row_data(0))
    row.responses = ["Loss AND Damage"]
    row.natural_hazards = ["Flood"]
    row.sectors = ["TransPort"]
    row.keywords = ["Hydrogen"]
    row.frameworks = ["adaptation"]
    row.instruments = ["Other|Governance"]
    row.document_type = "Act"

    result, metadata = build_metadata(taxonomy, row)

    assert result
    assert result.type == ResultType.RESOLVED
    assert (
        result.details
        == "Row 1 RESOLVED: {'Transport'}\nRow 1 RESOLVED: {'Adaptation'}\nRow 1 RESOLVED: {'Loss And Damage'}"
    )

    assert metadata
    assert set(metadata.keys()).symmetric_difference(METADATA_KEYS) == set([])
    assert metadata["topic"] == ["Loss And Damage"]
    assert metadata["hazard"] == ["Flood"]
    assert metadata["sector"] == ["Transport"]
    assert metadata["keyword"] == ["Hydrogen"]
    assert metadata["framework"] == ["Adaptation"]
    assert metadata["instrument"] == ["Other|Governance"]
    assert metadata["document_type"] == "Act"


def test_get_org_taxonomy__has_metadata_keys(test_db: Session):
    init_for_ingest(test_db)

    id, taxonomy = get_organisation_taxonomy(test_db, org_id=1)

    assert id
    assert taxonomy
    actual_keys = set(taxonomy.keys())

    assert actual_keys.symmetric_difference(METADATA_KEYS) == set([])


def test_get_org_taxonomy__raises_on_no_organisation(test_db: Session):
    init_for_ingest(test_db)

    with pytest.raises(NoResultFound):
        get_organisation_taxonomy(test_db, org_id=2)


def test_build_metadata__error_when_sector_notfound(test_db):
    init_for_ingest(test_db)
    _, taxonomy = get_organisation_taxonomy(test_db, org_id=1)
    row = IngestRow.from_row(1, get_ingest_row_data(0))
    row.sectors = ["Medical"]

    result, metadata = build_metadata(taxonomy, row)

    assert result
    assert result.type == ResultType.ERROR
    assert (
        result.details
        == "Row 1 has value(s) for 'sector' that is/are unrecognised: '{'Medical'}' "
    )

    assert metadata
    assert set(metadata.keys()).symmetric_difference(METADATA_KEYS) == set(["sector"])


def test_build_metadata__reports_when_resolved(test_db):
    init_for_ingest(test_db)
    _, taxonomy = get_organisation_taxonomy(test_db, org_id=1)
    row = IngestRow.from_row(1, get_ingest_row_data(0))
    row.sectors = ["Building"]

    result, metadata = build_metadata(taxonomy, row)

    assert result
    assert result.type == ResultType.RESOLVED
    assert result.details == "Row 1 RESOLVED: {'Buildings'}"

    assert metadata
    assert set(metadata.keys()).symmetric_difference(METADATA_KEYS) == set([])
    assert metadata["sector"] == ["Buildings"]
