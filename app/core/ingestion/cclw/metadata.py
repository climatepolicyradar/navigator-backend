from typing import Union

from app.core.ingestion.cclw.ingest_row_cclw import CCLWDocumentIngestRow
from app.db.models.law_policy.metadata import FamilyMetadata
from sqlalchemy.orm import Session
from app.core.ingestion.utils import Result, ResultType
from app.core.ingestion.metadata import Taxonomy, MetadataJson, build_metadata_field


MAP_OF_LIST_VALUES = {
    "sector": "sectors",
    "instrument": "instruments",
    "framework": "frameworks",
    "topic": "responses",
    "hazard": "natural_hazards",
    "keyword": "keywords",
}


def add_cclw_metadata(
    db: Session,
    family_import_id: str,
    taxonomy: Taxonomy,
    taxonomy_id: int,
    row: CCLWDocumentIngestRow,
) -> bool:
    result, metadata = build_cclw_metadata(taxonomy, row)
    if result.type == ResultType.ERROR:
        return False

    db.add(
        FamilyMetadata(
            family_import_id=family_import_id,
            taxonomy_id=taxonomy_id,
            value=metadata,
        )
    )
    return True


def build_cclw_metadata(
    taxonomy: Taxonomy, row: CCLWDocumentIngestRow
) -> tuple[Result, MetadataJson]:
    detail_list = []
    value: dict[str, Union[str, list[str]]] = {}
    num_fails = 0
    num_resolved = 0

    for tax_key, row_key in MAP_OF_LIST_VALUES.items():
        ingest_values = getattr(row, row_key)
        result, field_value = build_metadata_field(
            row.row_number, taxonomy, ingest_values, tax_key
        )

        if result.type == ResultType.OK:
            value[tax_key] = field_value
        elif result.type == ResultType.RESOLVED:
            value[tax_key] = field_value
            detail_list.append(result.details)
            num_resolved += 1
        else:
            detail_list.append(result.details)
            num_fails += 1

    row_result_type = ResultType.OK
    if num_resolved:
        row_result_type = ResultType.RESOLVED
    if num_fails:
        row_result_type = ResultType.ERROR

    return Result(type=row_result_type, details="\n".join(detail_list)), value