from returns.result import Failure, Result, Success

from app.extract.connectors import NavigatorDocument, NavigatorFamily
from app.models import (
    Document,
    DocumentDocumentRelationship,
    DocumentLabelRelationship,
    DocumentWithoutRelationships,
    Identified,
    Label,
)
from app.transform.models import CouldNotTransform, NoMatchingTransformations

mcf_projects_corpus_import_ids = [
    "MCF.corpus.AF.n0000",
    "MCF.corpus.CIF.n0000",
    "MCF.corpus.GEF.n0000",
    "MCF.corpus.GCF.n0000",
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
    match transform(input):
        case Success(d):
            return Success(d)

    return Failure(NoMatchingTransformations())


def transform(
    input: Identified[NavigatorFamily],
) -> Result[list[Document], CouldNotTransform]:
    documents: list[Document] = []

    """
    1) We have some document <=> family relationships that are essentially just a
    repeat of each other.

    We can use this 1-1 mapping and put the data from the family and store on the document.

    We will also need to generate the relationships between the related documents.

    At time of counting this was ~1096 documents.

    2) In the world of MCFs - we often have a "project document" that is a 1-1 mapping of the family.
    """
    is_version_of_document = next(
        (
            document
            for document in input.data.documents
            # 1)
            if document.title == input.data.title
            # 2)
            or (
                input.data.corpus.import_id in mcf_projects_corpus_import_ids
                and document.title.lower() == "project document"
            )
        ),
        None,
    )
    member_of_documents = [
        document
        for document in input.data.documents
        if not (
            is_version_of_document
            and document.import_id == is_version_of_document.import_id
        )
    ]

    """
    Transform
    """
    document_from_family = _transform_navigator_family(input.data)
    documents_from_documents = [
        _transform_navigator_document(
            document,
            input.data,
        )
        for document in member_of_documents
    ]

    """
    Relationships
    """
    document_from_family.relationships = [
        DocumentDocumentRelationship(
            type="has_member",
            document=DocumentWithoutRelationships(**document.model_dump()),
        )
        for document in documents_from_documents
    ]

    for document in documents_from_documents:
        document.relationships = [
            DocumentDocumentRelationship(
                type="member_of",
                document=DocumentWithoutRelationships(
                    **document_from_family.model_dump()
                ),
            )
        ]

    documents.append(document_from_family)
    documents.extend(documents_from_documents)

    """
    Versions
    """
    document_from_document: Document | None
    if is_version_of_document:
        document_from_document = _transform_navigator_document(
            is_version_of_document, input.data
        )
        document_from_document.relationships = [
            DocumentDocumentRelationship(
                type="is_version_of",
                document=DocumentWithoutRelationships(
                    **document_from_family.model_dump()
                ),
            )
        ]
        document_from_family.relationships.append(
            DocumentDocumentRelationship(
                type="has_version",
                document=DocumentWithoutRelationships(
                    **document_from_document.model_dump()
                ),
            ),
        )
        documents.append(document_from_document)
    else:
        # this is for debugging
        document_from_family.labels.append(
            DocumentLabelRelationship(
                type="debug",
                label=Label(
                    id="no_versions",
                    title="no_versions",
                    type="debug",
                ),
            )
        )

    return Success(documents)


def _transform_navigator_family(navigator_family: NavigatorFamily) -> Document:
    labels: list[DocumentLabelRelationship] = []

    if navigator_family.corpus.import_id == "Academic.corpus.Litigation.n0000":
        labels.append(
            DocumentLabelRelationship(
                type="entity_type",
                label=Label(
                    id="Legal case",
                    title="Legal case",
                    type="entity_type",
                ),
            )
        )

    if (
        navigator_family.corpus.import_id in mcf_projects_corpus_import_ids
        and len(navigator_family.documents) > 0
    ):
        labels.append(
            DocumentLabelRelationship(
                type="entity_type",
                label=Label(
                    id="Multilateral climate fund project",
                    title="Multilateral climate fund project",
                    type="entity_type",
                ),
            )
        )

    # this is for debugging
    if not labels:
        labels.append(
            DocumentLabelRelationship(
                type="debug",
                label=Label(
                    id="no_labels",
                    title="no_labels",
                    type="debug",
                ),
            )
        )

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
    """
    role = navigator_document.valid_metadata.get("role")
    if role is not None and len(role) > 0:
        normalised_role = role[0].capitalize()
        labels.append(
            DocumentLabelRelationship(
                type="entity_type",
                label=Label(
                    id=normalised_role, title=normalised_role, type="entity_type"
                ),
            )
        )

    return Document(
        id=navigator_document.import_id,
        title=navigator_document.title,
        labels=labels,
    )
