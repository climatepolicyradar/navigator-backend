from returns.result import Failure, Result, Success

from app.bootstrap_telemetry import get_logger, log_context
from app.extract.connectors import (
    NavigatorCollection,
    NavigatorDocument,
    NavigatorFamily,
)
from app.models import (
    Document,
    DocumentDocumentRelationship,
    DocumentLabelRelationship,
    DocumentWithoutRelationships,
    Identified,
    Label,
)
from app.transform.models import CouldNotTransform, NoMatchingTransformations

mcf_projects_corpus_types = [
    "AF",
    "CIF",
    "GEF",
    "GCF",
]

mcf_reports_corpus_import_ids = [
    "MCF.corpus.AF.Guidance",
    "MCF.corpus.CIF.Guidance",
    "MCF.corpus.GEF.Guidance",
    "MCF.corpus.GCF.Guidance",
]


def transform_navigator_family(
    input: Identified[NavigatorFamily],
) -> Result[list[Document], NoMatchingTransformations]:
    logger = get_logger()

    with log_context(import_id=input.id):
        logger.info(f"Transforming family with {len(input.data.documents)} documents")

        match transform(input):
            case Success(d):
                logger.info(f"Transform completed, produced {len(d)} documents")
                return Success(d)

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


def transform(
    input: Identified[NavigatorFamily],
) -> Result[list[Document], CouldNotTransform]:
    documents: list[Document] = []

    """
    Transform
    """
    document_from_family = _transform_navigator_family(input.data)
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
    document_from_family.relationships.extend(
        [
            DocumentDocumentRelationship(
                type="has_member",
                document=DocumentWithoutRelationships(**document.model_dump()),
            )
            for document in documents_from_documents
            if not _documents_match(document, document_from_family)
        ]
    )

    for document in documents_from_documents:
        if not _documents_match(document, document_from_family):
            document.relationships = [
                DocumentDocumentRelationship(
                    type="member_of",
                    document=DocumentWithoutRelationships(
                        **document_from_family.model_dump()
                    ),
                )
            ]

    """
    NavigatorCollections
    - document_from_collection -- has_member --> document_from_family
    - document_from_family -- member_of --> document_from_collection
    """
    document_from_family.relationships.extend(
        [
            DocumentDocumentRelationship(
                type="member_of",
                document=DocumentWithoutRelationships(**document.model_dump()),
            )
            for document in documents_from_collections
            if not _documents_match(document, document_from_family)
        ]
    )

    for document in documents_from_collections:
        if not _documents_match(document, document_from_family):
            document.relationships = [
                DocumentDocumentRelationship(
                    type="has_member",
                    document=DocumentWithoutRelationships(
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
    document_from_family.relationships.extend(
        DocumentDocumentRelationship(
            type="has_version",
            document=DocumentWithoutRelationships(**document.model_dump()),
        )
        for document in documents_from_documents
        if _documents_match(document, document_from_family)
    )
    for document in documents_from_documents:
        if _documents_match(document, document_from_family):
            document.relationships.append(
                DocumentDocumentRelationship(
                    type="is_version_of",
                    document=DocumentWithoutRelationships(
                        **document_from_family.model_dump()
                    ),
                )
            )

    # collections
    document_from_family.relationships.extend(
        DocumentDocumentRelationship(
            type="has_version",
            document=DocumentWithoutRelationships(**collection.model_dump()),
        )
        for collection in documents_from_collections
        if _documents_match(collection, document_from_family)
    )
    for collection in documents_from_collections:
        if _documents_match(collection, document_from_family):
            collection.relationships.append(
                DocumentDocumentRelationship(
                    type="is_version_of",
                    document=DocumentWithoutRelationships(
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
) -> list[DocumentLabelRelationship]:
    """
    Related ontologies
    @see: https://schema.org/provider
    """
    labels: list[DocumentLabelRelationship] = []
    organisation_to_agent_map = {
        "AF": "Adapation Fund",
        "CCLW": "Grantham Research Institute",
        "CIF": "The Climate Investment Funds",
        "CPR": "Policy Radar",
        "GCF": "Green Climate Fund",
        "GEF": "Global Environment Facility",
        "Ocean Energy Pathways": "Ocean Energy Pathway",
        "Sabin": "Sabin Center for Climate Change Law",
        "UNFCCC": "UNFCCC",
    }

    organisation_name = organisation_to_agent_map.get(
        navigator_family.corpus.organisation.name, "Unknown"
    )
    labels.append(
        DocumentLabelRelationship(
            type="provider",
            label=Label(
                type="agent",
                id=organisation_name,
                title=organisation_name,
            ),
        )
    )
    return labels


def _transform_navigator_family(navigator_family: NavigatorFamily) -> Document:
    labels: list[DocumentLabelRelationship] = []

    corpus_type_to_entity_type_map = {
        "Laws and Policies": "Laws and policies",
        "Litigation": "Legal case",
        "Reports": "Report",
        "Intl. agreements": "International agreement",
        "AF": "Multilateral climate fund project",
        "CIF": "Multilateral climate fund project",
        "GEF": "Multilateral climate fund project",
        "GCF": "Multilateral climate fund project",
    }
    labels = []

    # All families are currently canonical
    labels.append(
        DocumentLabelRelationship(
            type="status",
            label=Label(
                type="status",
                id="Canonical",
                title="Canonical",
            ),
        )
    )

    entity_type = corpus_type_to_entity_type_map.get(
        navigator_family.corpus.corpus_type.name
    )
    if entity_type:
        labels.append(
            DocumentLabelRelationship(
                type="entity_type",
                label=Label(
                    id=entity_type,
                    title=entity_type,
                    type="entity_type",
                ),
            )
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
            "Amended": "Amended",
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
            "Updated": "Updated",
        }

        event_type_to_activity_status_map = (
            mcf_project_event_type_to_activity_status_map
            | laws_and_policies_event_type_to_activity_status_map
        )

        for event in navigator_family.events:
            label_id = event_type_to_activity_status_map.get(
                event.event_type,
                "Unknown",
            )
            labels.append(
                DocumentLabelRelationship(
                    type="activity_status",
                    timestamp=event.date,
                    label=Label(
                        id=label_id,
                        title=label_id,
                        type="activity_status",
                    ),
                )
            )

    """
    Provider labels
    """
    labels.extend(_transform_family_corpus_organisation(navigator_family))

    return Document(
        id=navigator_family.import_id,
        title=navigator_family.title,
        labels=labels,
    )


def _transform_navigator_document(
    navigator_document: NavigatorDocument, navigator_family: NavigatorFamily
) -> Document:
    labels: list[DocumentLabelRelationship] = []

    if navigator_family.corpus.import_id == "Academic.corpus.Litigation.n0000":
        if navigator_document.events:
            event_type = navigator_document.events[0].event_type
            labels.append(
                DocumentLabelRelationship(
                    type="entity_type",
                    label=Label(
                        id=event_type,
                        title=event_type,
                        type="entity_type",
                    ),
                )
            )

    """
    These values are controlled
    @see: https://github.com/climatepolicyradar/data-migrations/blob/main/taxonomies/Intl.%20agreements.json#L42-L51
    @see: https://github.com/climatepolicyradar/data-migrations/blob/main/taxonomies/Laws%20and%20Policies.json#L381-L396
    """
    metadata_role = navigator_document.valid_metadata.get("role")
    if metadata_role is not None and len(metadata_role) > 0:
        normalised_role = metadata_role[0].capitalize()
        labels.append(
            DocumentLabelRelationship(
                type="entity_type",
                label=Label(
                    id=normalised_role, title=normalised_role, type="entity_type"
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
            DocumentLabelRelationship(
                type="entity_type",
                label=Label(
                    id=normalised_metadata_type,
                    title=normalised_metadata_type,
                    type="entity_type",
                ),
            )
        )

    """
    These were added to allow families to be parsed if they did not have any documents.
    """
    if navigator_document.import_id.endswith("placeholder"):
        labels.append(
            DocumentLabelRelationship(
                type="status",
                label=Label(id="Obsolete", title="Obsolete", type="status"),
            )
        )

    """
    Provider labels
    """
    labels.extend(_transform_family_corpus_organisation(navigator_family))

    return Document(
        id=navigator_document.import_id,
        title=navigator_document.title,
        labels=labels,
    )


def _transform_navigator_collection(
    navigator_collection: NavigatorCollection, navigator_family: NavigatorFamily
) -> Document:
    return Document(
        id=navigator_collection.import_id,
        title=navigator_collection.title,
        labels=[],
    )
