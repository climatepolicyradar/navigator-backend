from datetime import datetime

from data_in_models.models import (
    Document,
    DocumentRelationship,
    DocumentWithoutRelationships,
    Item,
    Label,
    LabelRelationship,
    LabelWithoutDocumentRelationships,
)
from returns.result import Failure, Result, Success

from app.bootstrap_telemetry import get_logger, log_context
from app.extract.connectors import (
    NavigatorCollection,
    NavigatorDocument,
    NavigatorFamily,
)
from app.geographies import geographies_lookup
from app.models import Identified, NavigatorConcept
from app.transform.models import CouldNotTransform, NoMatchingTransformations

mcf_projects_corpus_import_ids = [
    "MCF.corpus.AF.n0000",
    "MCF.corpus.CIF.n0000",
    "MCF.corpus.GCF.n0000",
    "MCF.corpus.GEF.n0000",
]

mcf_guidance_corpus_import_ids = [
    "MCF.corpus.AF.Guidance",
    "MCF.corpus.CIF.Guidance",
    "MCF.corpus.GCF.Guidance",
    "MCF.corpus.GEF.Guidance",
]

MCF_CORPORA = set(mcf_guidance_corpus_import_ids) | set(mcf_projects_corpus_import_ids)


LAWS_AND_POLICIES_CORPORA = {
    "CCLW.corpus.i00000001.n0000",
    "CPR.corpus.i00000001.n0000",
    "CPR.corpus.Goldstandard.n0000",
    "CPR.corpus.i00000589.n0000",
    "CPR.corpus.i00000591.n0000",
    "CPR.corpus.i00000592.n0000",
}

LITIGATION_CORPORA = {"Academic.corpus.Litigation.n0000"}


MCF_KEY_MAPPING = {"status": "project_status"}
MCF_EXCLUDED_KEYS = {"region", "external_id"}
MCF_ATTRIBUTE_KEYS = {
    "project_id",
    "project_url",
    "project_value_fund_spend",
    "project_value_co_financing",
    "approved_ref",
}


def transform_navigator_family(
    input: Identified[NavigatorFamily],
) -> Result[list[Document], CouldNotTransform | NoMatchingTransformations]:
    logger = get_logger()
    with log_context(import_id=input.id):
        logger.info(f"Transforming family with {len(input.data.documents)} documents")

        match transform(input):
            case Success(d):
                logger.info(f"Transform completed, produced {len(d)} documents")
                return Success(d)
            case Failure(error):
                logger.warning(f"Transformation failed: {error}")
                return Failure(error)

        logger.warning("No matching transformation found")
        return Failure(NoMatchingTransformations())


def _documents_match(
    documentA: Document,
    documentB: Document,
) -> bool:
    """
    In the world of MCFs - we often have a "project document" that is a 1-1 mapping of the family.
    """
    if documentA.title.lower() == "project document":
        return True

    """
    We have some document <=> family relationships that are essentially just a
    repeat of each other.

    We can use this 1-1 mapping and put the data from the family and store on the document.
    """
    return documentA.title.lower() == documentB.title.lower()


def _family_document_merged(
    navigator_family: NavigatorFamily, navigator_document: NavigatorDocument
) -> bool:
    if navigator_document.title.lower() == "project document":
        return True

    if navigator_family.title.lower() == navigator_document.title.lower():
        return True

    return False


def transform(
    input: Identified[NavigatorFamily],
) -> Result[list[Document], CouldNotTransform]:
    documents: list[Document] = []

    """
    Transform
    """
    result = _transform_navigator_family(input.data)
    if isinstance(result, Failure):
        return result
    else:
        document_from_family = result.unwrap()

    documents_from_documents = [
        _transform_navigator_document(
            document,
            input.data,
        )
        for document in input.data.documents
    ]
    documents_from_collections = [
        _transform_navigator_collection(
            collection,
            input.data,
        )
        for collection in input.data.collections
    ]

    """
    Relationships

    We are using the Dublin Core Metadata Initiative (DCMI) Metadata Terms vocabulary.
    @see: https://www.dublincore.org/specifications/dublin-core/dcmi-terms/#http://purl.org/dc/dcam/memberOf
    @see: https://www.dublincore.org/specifications/dublin-core/dcmi-terms/#http://purl.org/dc/dcam/hasMember

    `has_member` implies that this is the "container" document that has a member document.
    `member_of` implies that this is the "member" document that is a member of a container document.
    """

    """
    NavigatorDocuments
    - document_from_document -- member_of --> document_from_family
    - document_from_family -- has_member --> document_from_document
    """
    document_from_family.documents.extend(
        [
            DocumentRelationship(
                type="has_member",
                value=DocumentWithoutRelationships(**document.model_dump()),
            )
            for document in documents_from_documents
            if not _documents_match(document, document_from_family)
        ]
    )

    for document in documents_from_documents:
        if not _documents_match(document, document_from_family):
            document.documents = [
                DocumentRelationship(
                    type="member_of",
                    value=DocumentWithoutRelationships(
                        **document_from_family.model_dump()
                    ),
                )
            ]

    """
    NavigatorCollections
    - document_from_collection -- has_member --> document_from_family
    - document_from_family -- member_of --> document_from_collection
    """
    document_from_family.documents.extend(
        [
            DocumentRelationship(
                type="member_of",
                value=DocumentWithoutRelationships(**document.model_dump()),
            )
            for document in documents_from_collections
            if not _documents_match(document, document_from_family)
        ]
    )

    for document in documents_from_collections:
        if not _documents_match(document, document_from_family):
            document.documents = [
                DocumentRelationship(
                    type="has_member",
                    value=DocumentWithoutRelationships(
                        **document_from_family.model_dump()
                    ),
                )
            ]

    """
    Versions

    We are using the Dublin Core Metadata Initiative (DCMI) Metadata Terms vocabulary.
    @see: https://www.dublincore.org/specifications/dublin-core/dcmi-terms/#http://purl.org/dc/terms/isVersionOf
    @see: https://www.dublincore.org/specifications/dublin-core/dcmi-terms/#http://purl.org/dc/terms/hasVersion

    `has_version` implies that this is the `latest` version of the document.
    This might be better to have as an explicit flag or label, but it felt that this would do for the moment without adding extra complexity.

    `is_version_of` means that this is a version of document, which is useful for timetravelling, but is better represented as the `is_version_of` relationship.
    """

    # documents
    document_from_family.documents.extend(
        DocumentRelationship(
            type="has_version",
            value=DocumentWithoutRelationships(**document.model_dump()),
        )
        for document in documents_from_documents
        if _documents_match(document, document_from_family)
    )
    for document in documents_from_documents:
        if _documents_match(document, document_from_family):
            document.documents.append(
                DocumentRelationship(
                    type="is_version_of",
                    value=DocumentWithoutRelationships(
                        **document_from_family.model_dump()
                    ),
                )
            )

    # collections
    document_from_family.documents.extend(
        DocumentRelationship(
            type="has_version",
            value=DocumentWithoutRelationships(**collection.model_dump()),
        )
        for collection in documents_from_collections
        if _documents_match(collection, document_from_family)
    )
    for collection in documents_from_collections:
        if _documents_match(collection, document_from_family):
            collection.documents.append(
                DocumentRelationship(
                    type="is_version_of",
                    value=DocumentWithoutRelationships(
                        **document_from_family.model_dump()
                    ),
                )
            )

    """
    Return the documents
    """
    documents.append(document_from_family)
    documents.extend(documents_from_documents)
    documents.extend(documents_from_collections)

    return Success(documents)


def _transform_family_corpus_organisation(
    navigator_family: NavigatorFamily,
) -> list[LabelRelationship]:
    """
    Related ontologies
    @see: https://schema.org/provider
    """
    labels: list[LabelRelationship] = []
    corpus_to_provider_map = {
        "CCLW.corpus.i00000001.n0000": "Grantham Research Institute",
        "Academic.corpus.Litigation.n0000": "Sabin Center for Climate Change Law",
        "CPR.corpus.Goldstandard.n0000": "Gold Standard",
        "CPR.corpus.i00000589.n0000": "Naturebase",
        "CPR.corpus.i00000001.n0000": "NewClimate Institute",
        "CPR.corpus.i00000002.n0000": "Climate Policy Radar",
        "CPR.corpus.i00000591.n0000": "Laws Africa",
        "CPR.corpus.i00000592.n0000": "UNDRR",
        "MCF.corpus.AF.n0000": "Adaptation Fund",
        "MCF.corpus.AF.Guidance": "Adaptation Fund",
        "MCF.corpus.CIF.n0000": "The Climate Investment Funds",
        "MCF.corpus.CIF.Guidance": "The Climate Investment Funds",
        "MCF.corpus.GCF.n0000": "Green Climate Fund",
        "MCF.corpus.GCF.Guidance": "Green Climate Fund",
        "MCF.corpus.GEF.n0000": "Global Environment Facility",
        "MCF.corpus.GEF.Guidance": "Global Environment Facility",
        "OEP.corpus.i00000001.n0000": "Ocean Energy Pathways",
        "UNFCCC.corpus.i00000001.n0000": "UNFCCC",
        "UN.corpus.UNCCD.n0000": "UNCCD",
        "UN.corpus.UNCBD.n0000": "UNCBD",
        "ICCN.corpus.i00000001.n0000": "International Climate Councils Network",
    }

    provider_name = corpus_to_provider_map.get(
        navigator_family.corpus.import_id, "Unknown"
    )
    labels.append(
        LabelRelationship(
            type="provider",
            value=Label(
                type="agent",
                id=f"agent::{provider_name}",
                value=provider_name,
                attributes={
                    "attribution_url": navigator_family.corpus.attribution_url,
                    "corpus_text": navigator_family.corpus.corpus_text,
                    "corpus_image_url": (
                        f"https://cdn.climatepolicyradar.org/{navigator_family.corpus.corpus_image_url}"
                        if navigator_family.corpus.corpus_image_url
                        else ""
                    ),
                },
            ),
        )
    )
    return labels


def _transform_mcf_metadata(
    metadata: dict[str, list[str]],
) -> list[LabelRelationship]:
    labels: list[LabelRelationship] = []

    for key, values in metadata.items():
        if key in MCF_EXCLUDED_KEYS or key in MCF_ATTRIBUTE_KEYS:
            continue

        mapped_key = MCF_KEY_MAPPING.get(key, key)
        if mapped_key:
            labels.extend(
                LabelRelationship(
                    type=mapped_key,
                    value=Label(
                        id=f"{mapped_key}::{value}", value=value, type=mapped_key
                    ),
                )
                for value in values
            )

    return labels


def _transform_laws_policies_metadata(metadata: dict) -> list[LabelRelationship]:
    labels = []

    for key, values in metadata.items():
        if not values:
            continue

        for value in values:
            labels.append(
                LabelRelationship(
                    type=key,
                    value=Label(id=f"{key}::{value}", value=value, type=key),
                )
            )

    return labels


def _transform_metadata(navigator_family) -> list[LabelRelationship]:
    if not navigator_family.metadata:
        return []

    import_id = navigator_family.corpus.import_id

    if import_id in LAWS_AND_POLICIES_CORPORA:
        return _transform_laws_policies_metadata(navigator_family.metadata)

    if import_id in MCF_CORPORA:
        return _transform_mcf_metadata(navigator_family.metadata)

    return []


def _transform_metadata_to_attributes(
    navigator_family: NavigatorFamily,
) -> dict[str, str | float | bool]:
    if not navigator_family.metadata:
        return {}

    import_id = navigator_family.corpus.import_id

    if import_id in LITIGATION_CORPORA:
        return _transform_litigation_metadata_to_attributes(navigator_family.metadata)

    if import_id in MCF_CORPORA:
        return _transform_mcf_metadata_to_attributes(navigator_family.metadata)

    return {}


def _to_float(value: str) -> float:
    try:
        return float(value)
    except (ValueError, TypeError):
        return 0


def _identifier_attribute(key: str) -> str:
    return f"identifier::{key}"


def _float_attribute(key: str, unit: str) -> str:
    return f"{key}_{unit}"


def _transform_mcf_metadata_to_attributes(
    metadata: dict[str, list[str]],
) -> dict[str, str | float | bool]:
    attributes = {}

    project_id = metadata.get("project_id")
    if project_id and project_id[0]:
        attributes[_identifier_attribute("project_id")] = project_id[0]

    approved_ref = metadata.get("approved_ref")
    if approved_ref and approved_ref[0]:
        attributes[_identifier_attribute("project_approved_ref")] = approved_ref[0]

    project_value_fund_spend = metadata.get("project_value_fund_spend")
    if project_value_fund_spend and project_value_fund_spend[0]:
        attributes[_float_attribute("project_fund_spend", "usd")] = _to_float(
            project_value_fund_spend[0]
        )

    project_value_co_financing = metadata.get("project_value_co_financing")
    if project_value_co_financing and project_value_co_financing[0]:
        attributes[_float_attribute("project_co_financing", "usd")] = _to_float(
            project_value_co_financing[0]
        )

    project_url = metadata.get("project_url")
    if project_url and project_url[0]:
        attributes["project_url"] = project_url[0]

    return attributes


def _transform_litigation_metadata_to_attributes(
    metadata: dict[str, list[str]],
) -> dict[str, str | float | bool]:
    attributes = {}

    case_status = metadata.get("status")
    if case_status and case_status[0]:
        attributes["case_status"] = case_status[0]

    case_number = metadata.get("case_number")
    if case_number and case_number[0]:
        attributes[_identifier_attribute("case_number")] = case_number[0]

    provider_id = metadata.get("id")
    if provider_id and provider_id[0]:
        attributes[_identifier_attribute("provider_id")] = provider_id[0]

    core_object = metadata.get("core_object")
    if core_object and core_object[0]:
        attributes["core_object"] = core_object[0]

    original_case_name = metadata.get("original_case_name")
    if original_case_name and original_case_name[0]:
        attributes["original_case_name"] = original_case_name[0]

    return attributes


def _transform_geographies(
    navigator_family: NavigatorFamily,
) -> list[LabelRelationship]:
    logger = get_logger()
    labels = []
    if navigator_family.geographies:
        geography_labels = []
        for geograpy_id in navigator_family.geographies:
            # We exclude No Geography (XAA) as this was used as `geography` was previously required.
            # An empty list is a clearer depiction of a document not having a geography.
            if geograpy_id != "XAA":
                geography = geographies_lookup.get(geograpy_id)
                if geography:
                    geography_labels.append(
                        LabelRelationship(
                            type="geography",
                            value=Label(
                                id=f"geography::{geography.id}",
                                value=geography.name,
                                type="geography",
                            ),
                        )
                    )
            else:
                logger.warning(f"Geography not found: {geograpy_id}")
        labels.extend(geography_labels)

    return labels


def _shallow_label(
    label: LabelWithoutDocumentRelationships,
) -> LabelWithoutDocumentRelationships:
    """
    We want to avoid deep nesting of labels and recursive references.
    """
    return LabelWithoutDocumentRelationships(
        id=label.id,
        type=label.type,
        value=label.value,
        labels=[],
    )


def _transform_litigation_concepts_to_label_relationships(
    concepts: list[NavigatorConcept],
    family_import_id: str,
) -> Result[list[LabelRelationship], CouldNotTransform]:
    """
    Convert litigation concepts into label relationships with subconcept hierarchies.

    Returns:
        List[LabelRelationship] where each:
        - type="concept"
        - value=LabelWithoutRelationships (with nested .labels for hierarchies)
        - family_import_id= the import_id of the family these concepts belong to, to debug if needed.
        - Parent references are SHALLOW (labels=[] to prevent deep nesting).
    """

    # The relation values unfortunately conflict with other values in the taxonomy, so we have to map them as `legal`
    # @see: https://github.com/climatepolicyradar/litigation-data-mapper/blob/49e8da8f4449dc8e3fec5a126b9973df4efb4d26/litigation_data_mapper/extract_concepts.py#L45
    relation_to_type_map = {
        "author": "legal",
        "jurisdiction": "jurisdiction",
        "category": "case_category",  # This is the main conflict - category is reserved for higher level types like "Law", "Policy", etc.
        "principal_law": "principal_law",
    }

    # Build core labels indexed by (relation, id) - using a tuple here as the ids may not be unique across different concept types (relations)
    label_map: dict[tuple[str, str], LabelWithoutDocumentRelationships] = {
        (c.relation, c.id): LabelWithoutDocumentRelationships(
            id=f"{relation_to_type_map.get(c.relation, 'litigation_concept')}::{c.id}",
            type=relation_to_type_map.get(c.relation, "litigation_concept"),
            value=c.preferred_label,
        )
        for c in concepts
    }

    # Secondary index for parent lookups by preferred_label
    label_by_name: dict[tuple[str, str], LabelWithoutDocumentRelationships] = {
        (c.relation, c.preferred_label): label_map[(c.relation, c.id)] for c in concepts
    }

    # Wire up parent-child relationships
    for concept in concepts:
        child = label_map[(concept.relation, concept.id)]
        for parent_name in concept.subconcept_of_labels:
            parent = label_by_name.get((concept.relation, parent_name))
            if parent is None:
                continue
            else:
                parent_ref = _shallow_label(parent)

                child.labels.append(
                    LabelRelationship(type="subconcept_of", value=parent_ref)
                )

    return Success(
        [
            # we use legal_concept over concept as `concept` is reserved for our knowledge graph labels
            LabelRelationship(type="legal_concept", value=label)
            for label in label_map.values()
        ]
    )


def _transform_to_category(
    navigator_family: NavigatorFamily,
) -> list[LabelRelationship]:
    labels = []

    un_submission_corpora = [
        "UNFCCC.corpus.i00000001.n0000",
        "UN.corpus.UNCCD.n0000",
        "UN.corpus.UNCBD.n0000",
    ]
    if navigator_family.corpus.import_id in un_submission_corpora:
        labels.append(
            LabelRelationship(
                type="category",
                value=Label(
                    id="category::UN submission",
                    value="UN submission",
                    type="category",
                ),
            )
        )

    oep_corpora = [
        "OEP.corpus.i00000001.n0000",
    ]
    if navigator_family.corpus.import_id in oep_corpora:
        labels.append(
            LabelRelationship(
                type="category",
                value=Label(
                    id="category::Report",
                    value="Report",
                    type="category",
                ),
            )
        )

    litigation_corpora = [
        "Academic.corpus.Litigation.n0000",
    ]
    if navigator_family.corpus.import_id in litigation_corpora:
        labels.append(
            LabelRelationship(
                type="category",
                value=Label(
                    id="category::Litigation",
                    value="Litigation",
                    type="category",
                ),
            )
        )

    corporate_disclosures_corpora = [
        "CPR.corpus.i00000002.n0000",
    ]
    if navigator_family.corpus.import_id in corporate_disclosures_corpora:
        labels.append(
            LabelRelationship(
                type="category",
                value=Label(
                    id="category::Corporate Disclosure",
                    value="Corporate Disclosure",
                    type="category",
                ),
            )
        )

    if navigator_family.corpus.import_id in MCF_CORPORA:
        labels.append(
            LabelRelationship(
                type="category",
                value=Label(
                    id="category::Multilateral Climate Fund project",
                    value="Multilateral Climate Fund project",
                    type="category",
                ),
            )
        )

        if navigator_family.corpus.import_id in mcf_guidance_corpus_import_ids:
            labels.append(
                LabelRelationship(
                    type="entity_type",
                    value=Label(
                        id="entity_type::Guidance",
                        value="Guidance",
                        type="entity_type",
                    ),
                )
            )
        if navigator_family.corpus.import_id in mcf_projects_corpus_import_ids:
            labels.append(
                LabelRelationship(
                    type="entity_type",
                    value=Label(
                        id="entity_type::Project",
                        value="Project",
                        type="entity_type",
                    ),
                )
            )

    if navigator_family.corpus.corpus_type.name == "Laws and Policies":
        # We are maintaing this as the assumption is all Laws and policies
        # have been tagged as "LEGISLATIVE" OR "EXECUTIVE", but there is a possiblity
        # that they have not as the system allows it. This should allow us
        # to assess that data.
        labels.append(
            LabelRelationship(
                type="deprecated_category",
                value=Label(
                    id="deprecated_category::Laws and Policies",
                    value="Laws and Policies",
                    type="deprecated_category",
                ),
            )
        )
        if navigator_family.category == "LEGISLATIVE":
            labels.append(
                LabelRelationship(
                    type="category",
                    value=Label(
                        id="category::Law",
                        value="Law",
                        type="category",
                    ),
                )
            )
        if navigator_family.category == "EXECUTIVE":
            labels.append(
                LabelRelationship(
                    type="category",
                    value=Label(
                        id="category::Policy",
                        value="Policy",
                        type="category",
                    ),
                )
            )

    return labels


def _transform_litigation_data(
    navigator_family: NavigatorFamily,
) -> Result[list[LabelRelationship], CouldNotTransform]:
    """
    Transform litigation-specific concepts and filing date into label relationships.
    Only applies to the Litigation corpus.
    """
    labels: list[LabelRelationship] = []

    if navigator_family.concepts:
        match _transform_litigation_concepts_to_label_relationships(
            navigator_family.concepts, navigator_family.import_id
        ):
            case Success(litigation_labels):
                labels.extend(litigation_labels)
            case Failure(e):
                return Failure(e)

    if navigator_family.events:
        filing_event = next(
            (
                e
                for e in navigator_family.events
                if e.event_type == "Filing Year For Action"
            ),
            None,
        )
        if filing_event:
            labels.append(
                LabelRelationship(
                    type="activity_status",
                    timestamp=filing_event.date,
                    value=Label(
                        id="activity_status::Filed",
                        value="Filed",
                        type="activity_status",
                    ),
                )
            )

    return Success(labels)


def _part_of_global_stock_take_1(navigator_family: NavigatorFamily) -> bool:
    """
    Checks if a family was fart of the first Global Stocktake (GST1).
    """
    gst1_party_submission_date = "2023-11-30"

    family_created_date_without_time = str(
        datetime.fromisoformat(navigator_family.created.replace("Z", "+00:00")).date()
    )

    gst1_party_submission = (
        navigator_family.corpus.import_id == "UNFCCC.corpus.i00000001.n0000"
        and navigator_family.metadata.get("author_type") is not None
        and navigator_family.metadata["author_type"][0] == "Party"
        and family_created_date_without_time == gst1_party_submission_date
    )

    gst1_non_party_report_dates = [
        "2023-11-30",
        "2024-10-15",
        "2024-10-17",
        "2024-11-06",
        "2024-11-15",
        "2024-11-18",
    ]

    gst1_non_party_report = (
        navigator_family.corpus.import_id == "UNFCCC.corpus.i00000001.n0000"
        and navigator_family.metadata.get("author_type") is not None
        and navigator_family.metadata["author_type"][0] == "Non-Party"
        and family_created_date_without_time in gst1_non_party_report_dates
    )

    return gst1_party_submission or gst1_non_party_report


# trunk-ignore(ruff/PLR0912)
def _transform_navigator_family(
    navigator_family: NavigatorFamily,
) -> Result[Document, CouldNotTransform]:
    labels: list[LabelRelationship] = []
    attributes: dict[str, str | float | bool] = {}

    """
    All families are currently Principal.
    Based on the FRBR taxonomy.

    @see: https://en.wikipedia.org/wiki/Functional_Requirements_for_Bibliographic_Records
    @see: https://developers.laws.africa/get-started/works-and-expressions
    """
    labels.append(
        LabelRelationship(
            type="status",
            value=Label(
                type="status",
                id="status::Principal",
                value="Principal",
            ),
        )
    )

    # MCF reports and guidance labels
    if navigator_family.corpus.import_id in mcf_projects_corpus_import_ids:
        labels.append(
            LabelRelationship(
                type="entity_type",
                value=Label(
                    id="entity_type::Project",
                    value="Project",
                    type="entity_type",
                ),
            )
        )

    if navigator_family.corpus.import_id in mcf_guidance_corpus_import_ids:
        labels.append(
            LabelRelationship(
                type="entity_type",
                value=Label(
                    id="entity_type::Guidance",
                    value="Guidance",
                    type="entity_type",
                ),
            )
        )

    # GST1 labels
    if _part_of_global_stock_take_1(navigator_family):
        labels.append(
            LabelRelationship(
                type="process",
                value=Label(
                    id="process::GST1",
                    value="GST1 Submission",
                    type="process",
                ),
            ),
        )

    # We skip litigation as we hijacked the event_type for document type
    if navigator_family.corpus.import_id != "Academic.corpus.Litigation.n0000":
        """
        Activity status

        This is loosely inspired by the IATI ontology
        @see: https://iatistandard.org/en/iati-standard/203/activity-standard/iati-activities/iati-activity/activity-status/
        @see: https://iatistandard.org/en/iati-standard/203/codelists/activitystatus/

        e.g.
        Project Approved => Pipeline/identification
        Under Implementation => Implementation
        Project Completed => Closed
        """

        """
        Values from Navigator are controlled
        @see: https://github.com/climatepolicyradar/data-migrations/blob/main/taxonomies/AF.json#L7C9-L9C28
        @see: https://github.com/climatepolicyradar/data-migrations/blob/main/taxonomies/CIF.json#L7-L10
        @see: https://github.com/climatepolicyradar/data-migrations/blob/main/taxonomies/CIF.json#L7-L10
        @see: https://github.com/climatepolicyradar/data-migrations/blob/main/taxonomies/GEF.json#L7-L11
        """
        mcf_project_event_type_to_activity_status_map = {
            "Concept Approved": "Concept approved",
            "Project Approved": "Approved",
            "Under Implementation": "Under implementation",
            "Project Completed": "Completed",
            "Cancelled": "Cancelled",
        }

        """
        Values from Navigator are controlled
        @see: https://github.com/climatepolicyradar/data-migrations/blob/main/taxonomies/Laws%20and%20Policies.json#L17-L33
        @see: https://github.com/climatepolicyradar/data-migrations/blob/main/taxonomies/Intl.%20agreements.json#L7-L24
        """
        laws_and_policies_event_type_to_activity_status_map = {
            "Amended": "Amended/Updated",
            "Appealed": "Appealed",
            "Closed": "Closed",
            "Declaration Of Climate Emergency": "Declaration of climate emergency",
            "Dismissed": "Dismissed",
            "Entered Into Force": "Entered into force",
            "Filing": "Filing",
            "Granted": "Granted",
            "Implementation Details": "Implementation details",
            "International Agreement": "International agreement",
            "Net Zero Pledge": "Net zero pledge",
            "Other": "Other",
            "Passed/Approved": "Passed/Approved",
            "Repealed/Replaced": "Repealed/Replaced",
            "Set": "Set",
            "Settled": "Settled",
            "Updated": "Amended/Updated",
        }

        """
        Values from Navigator are controlled
        @see: https://github.com/climatepolicyradar/data-migrations/blob/main/taxonomies/Reports.json#L6
        """
        reports_event_type_to_activity_status_map = {
            "Published": "Published",
        }

        event_type_to_activity_status_map = (
            mcf_project_event_type_to_activity_status_map
            | laws_and_policies_event_type_to_activity_status_map
            | reports_event_type_to_activity_status_map
        )

        for event in navigator_family.events:
            event_type_label_id = event_type_to_activity_status_map.get(
                event.event_type,
                "Unknown event_type",
            )
            labels.append(
                LabelRelationship(
                    type="activity_status",
                    timestamp=event.date,
                    value=Label(
                        id=f"activity_status::{event_type_label_id}",
                        value=event_type_label_id,
                        type="activity_status",
                    ),
                )
            )

    """
    Provider labels
    """
    labels.extend(_transform_family_corpus_organisation(navigator_family))

    """
    Geography labels
    """
    labels.extend(_transform_geographies(navigator_family))

    """
    metadata.author and metadata.author_type
    """
    author_metadata = navigator_family.metadata.get("author")
    if author_metadata and author_metadata[0]:
        author = author_metadata[0]
        author_type = "agent"
        author_type_metadata = navigator_family.metadata.get("author_type")
        if author_type_metadata and author_type_metadata[0]:
            author_type = author_type_metadata[0].lower()

        labels.append(
            LabelRelationship(
                type="author",
                value=Label(
                    id=f"{author_type}::{author}",
                    value=author,
                    type=author_type,
                ),
            )
        )

    """
    family.category
    @see: https://github.com/climatepolicyradar/navigator-db-client/blob/a842d5e971894246843c1915de9179ddd991b25c/db_client/models/dfce/family.py#L67-L75

    This is used on the frontend for now, but we will be removing it for the newly implemented canonical category below.
    """
    labels.append(
        LabelRelationship(
            type="deprecated_category",
            value=Label(
                id=f"deprecated_category::{navigator_family.category}",
                value=navigator_family.category,
                type="deprecated_category",
            ),
        )
    )

    """
    Canonical category
    """
    labels.extend(_transform_to_category(navigator_family))

    """
    Slug
    We should not couple to this implementation as it is an incomplete ID service which is unmaintained.
    But we need it for migration purposes.
    """
    attributes["deprecated_slug"] = navigator_family.slug

    """
    Metadata
    """

    labels.extend(_transform_metadata(navigator_family))

    attributes.update(_transform_metadata_to_attributes(navigator_family))

    """
    Litigation concepts, not to be confused with other concepts these are defined by the
    Sabin Center for Climate Change Law and only apply to the Academic.corpus.Litigation.n0000 corpus.
    We are adding also adding Litigation filing date as an attribute on the family as it is a key date
    for litigation documents.
    """

    if navigator_family.corpus.import_id == "Academic.corpus.Litigation.n0000":
        match _transform_litigation_data(navigator_family):
            case Success(litigation_labels):
                labels.extend(litigation_labels)
            case Failure(e):
                return Failure(e)

    """Dates"""
    if navigator_family.published_date:
        attributes["published_date"] = navigator_family.published_date
    if navigator_family.last_updated_date:
        attributes["last_updated_date"] = navigator_family.last_updated_date

    """This field defines whether a document is available to be searched in Vespa.
    It is still used to filter out un-published or deleted documents in the frontend.
    We are mapping it onto principal documents that have at least one related document
    that has a 'PUBLISHED' status to keep the data-in-api clean. For simplicity, we do not
    add a status if the family cannot be considered published.
    """

    contains_published_document = [
        doc for doc in navigator_family.documents if doc.document_status == "published"
    ]
    if navigator_family.documents and contains_published_document:
        attributes["status"] = "published"

    return Success(
        Document(
            id=navigator_family.import_id,
            title=navigator_family.title,
            description=navigator_family.summary,
            labels=_deduplicate_labels(labels),
            attributes=attributes,
        )
    )


def _transform_document_urls(navigator_document):
    items = []
    if navigator_document.cdn_object is not None:
        items.append(
            Item(
                url=navigator_document.cdn_object,
                type="cdn",
                content_type=navigator_document.content_type,
            )
        )
    if navigator_document.source_url is not None:
        items.append(
            Item(
                url=navigator_document.source_url,
                type="source",
                content_type=navigator_document.content_type,
            )
        )
    return items


# trunk-ignore(ruff/PLR0912)
def _transform_navigator_document(
    navigator_document: NavigatorDocument, navigator_family: NavigatorFamily
) -> Document:
    labels: list[LabelRelationship] = []
    attributes: dict[str, str | float | bool] = {}
    description = None

    if navigator_family.corpus.import_id == "Academic.corpus.Litigation.n0000":
        if navigator_document.events:
            document_event = navigator_document.events[0]
            description = document_event.metadata.get("description")
            event_type = document_event.event_type
            labels.append(
                LabelRelationship(
                    type="entity_type",
                    value=Label(
                        id=f"entity_type::{event_type}",
                        value=event_type,
                        type="entity_type",
                    ),
                )
            )
            labels.append(
                LabelRelationship(
                    type="activity_status",
                    timestamp=document_event.date,
                    value=Label(
                        id="activity_status::Filed",
                        value="Filed",
                        type="activity_status",
                    ),
                )
            )
            action_taken = document_event.metadata.get("action_taken")
            if action_taken:
                attributes["action_taken"] = action_taken[0]

    """
    These values are controlled
    @see: https://github.com/climatepolicyradar/data-migrations/blob/main/taxonomies/Intl.%20agreements.json#L42-L51
    @see: https://github.com/climatepolicyradar/data-migrations/blob/main/taxonomies/Laws%20and%20Policies.json#L381-L396
    """
    metadata_role = navigator_document.valid_metadata.get("role")
    if metadata_role is not None and len(metadata_role) > 0:
        normalised_role = metadata_role[0].capitalize()
        labels.append(
            LabelRelationship(
                type="role",
                value=Label(
                    id=f"entity_type::{normalised_role}",
                    value=normalised_role,
                    type="entity_type",
                ),
            )
        )

    """
    These values are controlled
    @see: https://github.com/climatepolicyradar/data-migrations/blob/main/taxonomies/Reports.json#L16-L26
    @see: https://github.com/climatepolicyradar/data-migrations/blob/main/taxonomies/GCF.json#L52-L67
    @see: https://github.com/climatepolicyradar/data-migrations/blob/main/taxonomies/Intl.%20agreements.json#L54-L144
    """
    metadata_type = navigator_document.valid_metadata.get("type")
    if metadata_type is not None and len(metadata_type) > 0:
        normalised_metadata_type = metadata_type[0].capitalize()
        labels.append(
            LabelRelationship(
                type="entity_type",
                value=Label(
                    id=f"entity_type::{normalised_metadata_type}",
                    value=normalised_metadata_type,
                    type="entity_type",
                ),
            )
        )

    """
    These were added to allow families to be parsed if they did not have any documents.
    """
    if navigator_document.import_id.endswith("placeholder"):
        labels.append(
            LabelRelationship(
                type="status",
                value=Label(id="status::Obsolete", value="Obsolete", type="status"),
            )
        )

    """
    Provider labels
    """
    labels.extend(_transform_family_corpus_organisation(navigator_family))

    # GST1 labels
    if _part_of_global_stock_take_1(navigator_family):
        labels.append(
            LabelRelationship(
                type="process",
                value=Label(
                    id="process::GST1",
                    value="GST1 Submission",
                    type="process",
                ),
            ),
        )

    """
    Items
    """
    items: list[Item] = []

    items.extend(_transform_document_urls(navigator_document))

    """
    Slug
    We should not couple to this implementation as it is an incomplete ID service which is unmaintained.
    But we need it for migration purposes.
    """
    attributes["deprecated_slug"] = navigator_document.slug

    """
    Geography labels
    """
    labels.extend(_transform_geographies(navigator_family))

    """
    Canonical category
    """
    labels.extend(_transform_to_category(navigator_family))

    """
    Language labels
    """
    for lang in navigator_document.languages:
        labels.append(
            LabelRelationship(
                type="language",
                value=Label(id=f"language::{lang}", value=lang, type="language"),
            )
        )

    """
    Internal attributes
    """

    if navigator_document.variant:
        attributes["variant"] = navigator_document.variant
    if navigator_document.md5_sum:
        attributes["md5_sum"] = navigator_document.md5_sum

    """This field defines whether a document is available to be searched in Vespa.
    It is still used to filter out un-published or deleted documents in the frontend."""
    attributes["status"] = navigator_document.document_status

    """Dates"""
    if navigator_family.published_date:
        attributes["published_date"] = navigator_family.published_date
    if navigator_family.last_updated_date:
        attributes["last_updated_date"] = navigator_family.last_updated_date

    # Merged - experimental and should not be relied upon
    if _family_document_merged(navigator_family, navigator_document):
        labels.append(
            LabelRelationship(
                type="status",
                value=Label(id="status::Merged", value="Merged", type="status"),
            )
        )

    return Document(
        id=navigator_document.import_id,
        title=navigator_document.title,
        description=description[0] if description else None,
        labels=_deduplicate_labels(labels),
        items=items,
        attributes=attributes,
    )


def _transform_navigator_collection(
    navigator_collection: NavigatorCollection, navigator_family: NavigatorFamily
) -> Document:
    labels: list[LabelRelationship] = []

    labels.append(
        LabelRelationship(
            type="entity_type",
            value=Label(
                id="entity_type::Collection",
                value="Collection",
                type="entity_type",
            ),
        )
    )

    attributes = {}

    if navigator_collection.slug:
        attributes["deprecated_slug"] = navigator_collection.slug

    """Dates"""
    if navigator_collection.created:
        attributes["published_date"] = navigator_collection.created
    if navigator_collection.last_modified:
        attributes["last_updated_date"] = navigator_collection.last_modified

    return Document(
        id=navigator_collection.import_id,
        title=navigator_collection.title,
        labels=labels,
        attributes=attributes,
    )


def _deduplicate_labels(
    labels: list[LabelRelationship],
) -> list[LabelRelationship]:
    """
    Deduplicate labels by (label.id, type) pair, keeping first occurrence.

    This prevents database constraint violations on the composite primary key
    (document_id, label_id) in the DocumentLabelLink table.
    """
    dedup_keys = set()
    unique_labels = []
    for label_rel in labels:
        key = (label_rel.value.id, label_rel.type)
        if key in dedup_keys:
            continue
        unique_labels.append(label_rel)
        dedup_keys.add(key)
    return unique_labels
