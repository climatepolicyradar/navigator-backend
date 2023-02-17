from typing import Set, cast

from sqlalchemy.orm import Session

from app.db.models.law_policy.metadata import FamilyMetadata
from scripts.ingest_dfc.dfc.match import match_unknown_value
from scripts.ingest_dfc.utils import DfcRow, Result, ResultType

MAP_OF_LIST_VALUES = {
    "sector": "sectors",
    "instrument": "instruments",
    "framework": "frameworks",
    "topic": "responses",
    "hazard": "natural_hazards",
    "keyword": "keywords",
}

MAP_OF_STR_VALUES = {
    "document_type": "document_type",
}


def add_metadata(
    db: Session, family_import_id: str, taxonomy: dict, taxonomy_id: int, row: DfcRow
) -> bool:
    result, metadata = build_metadata(taxonomy, row)
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


def build_metadata(taxonomy: dict, row: DfcRow) -> tuple[Result, dict]:
    detail_list = []
    value = {}
    num_fails = 0
    num_resolved = 0

    # FIXME: Still todo is ... document_type: str
    for tax_key, row_key in MAP_OF_LIST_VALUES.items():
        result, field_value = build_metadata_field(taxonomy, row, tax_key, row_key)

        if result.type == ResultType.OK:
            value[tax_key] = field_value
        elif result.type == ResultType.RESOLVED:
            value[tax_key] = field_value
            detail_list.append(result.details)
            num_resolved += 1
        else:
            detail_list.append(result.details)
            num_fails += 1

    for tax_key, row_key in MAP_OF_STR_VALUES.items():
        row_value = getattr(row, row_key)
        allowed_values = taxonomy[tax_key]["allowed_values"]
        result = Result()
        if row_value in allowed_values:
            value[tax_key] = row_value
        else:
            suggestion = match_unknown_value(row_value, set(allowed_values))
            if not suggestion:
                result.type = ResultType.ERROR
                detail_list.append(
                    f"Found no matches for {row_value} in {allowed_values}"
                )
                num_fails += 1
            else:
                value[tax_key] = suggestion
                result.type = ResultType.RESOLVED
                detail_list.append(f"Resolved {row_value} to {suggestion}")
                num_resolved += 1

    row_result_type = (
        ResultType.ERROR
        if num_fails
        else ResultType.RESOLVED
        if num_resolved
        else ResultType.OK
    )
    return Result(type=row_result_type, details="\n".join(detail_list)), value


def build_metadata_field(
    taxonomy: dict, row: DfcRow, tax_key: str, row_key: str
) -> tuple[Result, dict]:
    ingest_values = getattr(row, row_key)
    row_set = set(ingest_values)
    allowed_set = set(taxonomy[tax_key]["allowed_values"])
    allow_blanks = cast(bool, taxonomy[tax_key]["allow_blanks"])

    if len(row_set) == 0:
        if not allow_blanks:
            details = (
                f"Row {row.row_number} is blank for {tax_key} - which is not allowed."
            )
            return Result(type=ResultType.ERROR, details=details), {}
        return Result(), {}  # field is blank and allowed

    unknown_set = row_set.difference(allowed_set)
    if not unknown_set:
        return Result(), {tax_key: ingest_values}  # all is well - everything found

    resolved_set = resolve_unknown(unknown_set, allowed_set)

    if len(resolved_set) == len(unknown_set):
        details = f"Row {row.row_number} RESOLVED: {resolved_set}"
        vals = row_set.difference(unknown_set).union(resolved_set)
        return Result(type=ResultType.RESOLVED, details=details), {tax_key: list(vals)}

    # If we get here we have not managed to resolve the unknown values.

    details = (
        f"Row {row.row_number} has value(s) for '{tax_key}' that is/are "
        f"unrecognised: '{unknown_set}' "
    )

    if len(resolved_set):
        details += f"able to resolve: {resolved_set}"

    return Result(type=ResultType.ERROR, details=details), {}


def resolve_unknown(unknown_set: Set, allowed_set: Set) -> Set[str]:
    suggestions = set()
    for unknown_value in unknown_set:
        suggestion = match_unknown_value(unknown_value, allowed_set)
        if suggestion:
            suggestions.add(suggestion)
    return suggestions
