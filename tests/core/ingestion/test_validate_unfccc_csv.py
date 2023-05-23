from typing import cast
from app.core.ingestion.processor import initialise_context
from app.core.ingestion.unfccc.validate import validate_unfccc_csv
from app.core.ingestion.utils import UNFCCCIngestContext
from tests.core.ingestion.helpers import populate_for_ingest


ONE_UNFCCC_ROW = """Category,Submission Type,Family Name,Document Title,Documents,Author,Author Type,Geography,Geography ISO,Date,Document Role,Document Variant,Language,Download URL,CPR Collection ID,CPR Document ID,CPR Document Slug,CPR Family ID,CPR Family Slug,CPR Document Status
UNFCCC,Synthesis Report,Nationally determined contributions under the Paris Agreement. Revised note by the secretariat,Nationally determined contributions under the Paris Agreement. Revised note by the secretariat,https://unfccc.int/sites/default/files/resource/cma2021_08r01_S.pdf,UNFCCC Secretariat,Party,UK,GBR,2021-10-25T12:00:00Z,,,en,url of downloaded document,UNFCCC.Collection.Found1;UNFCCC.Collection.Found2,UNFCCC.Document.1,Doc-slug,UNFCCC.family.1,Family-slug,
"""


TWO_COLLECTION_ROW = """CPR Collection ID,Collection name,Collection summary
UNFCCC.Collection.Found1,Collection One,Everything to do with testing
UNFCCC.Collection.Found2,Collection One,Everything to do with testing
"""


def test_validate_unfccc_csv(test_db):
    results = []
    populate_for_ingest(test_db)
    test_db.commit()
    ctx = initialise_context(test_db, "UNFCCC")
    message = validate_unfccc_csv(
        ONE_UNFCCC_ROW,
        TWO_COLLECTION_ROW,
        test_db,
        cast(UNFCCCIngestContext, ctx),
        results,
    )

    assert message == "UNFCCC validation result: 1 Rows, 0 Failures, 0 Resolved"
