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


class TransformerLabel(Label):
    id: str
    title: str
    type: str = "transformer"


def transform_navigator_family(
    input: Identified[NavigatorFamily],
) -> Result[list[Document], NoMatchingTransformations]:
    # order here matters - if there is a match, we will stop searching for a transformer
    transformers = [
        transform_navigator_family_with_litigation_corpus_type,
        transform_navigator_family_with_single_matching_document,
        transform_navigator_family_with_matching_document_title_and_related_documents,
        transform_navigator_family_never,
    ]

    success = None
    failures = []
    for transformer in transformers:
        result = transformer(input)
        match result:
            case Success(document):
                success = document
                break
            case Failure(error):
                failures.append(error)

    if success:
        return Success(success)
    else:
        return Failure(
            NoMatchingTransformations(
                f"No matching transformations found for {input.id}"
            )
        )


def transform_navigator_family_with_single_matching_document(
    input: Identified[NavigatorFamily],
) -> Result[list[Document], CouldNotTransform]:
    """
    We have some document <=> family relationships that are essentially just a
    repeat of each other.

    We can use this 1-1 mapping and put the data from the family and store on the document.

    At time of counting this was ~9000 documents.
    """
    if (
        len(input.data.documents) == 1
        and input.data.documents[0].title == input.data.title
    ):
        return Success(
            [
                transform_navigator_family_document(
                    input,
                    input.data.documents[0],
                    "transform_navigator_family_with_single_matching_document",
                )
            ]
        )
    else:
        return Failure(
            CouldNotTransform(
                f"transform_navigator_family_with_single_matching_document could not transform {input.id}"
            )
        )


def transform_navigator_family_with_matching_document_title_and_related_documents(
    input: Identified[NavigatorFamily],
) -> Result[list[Document], CouldNotTransform]:
    """
    We have some document <=> family relationships that are essentially just a
    repeat of each other.

    We can use this 1-1 mapping and put the data from the family and store on the document.

    We will also need to generate the relationships between the related documents.

    At time of counting this was ~1096 documents.
    """
    if len(input.data.documents) > 1:
        matching_document = next(
            (
                document
                for document in input.data.documents
                if document.title == input.data.title
            ),
            None,
        )
        if matching_document:
            """
            Transform
            """
            transformed_matching_document = transform_navigator_family_document(
                input,
                matching_document,
                "transform_navigator_family_with_matching_document_title_and_related_documents",
            )
            transformed_related_documents = [
                transform_navigator_family_document(
                    input,
                    document,
                    "transform_navigator_family_with_matching_document_title_and_related_documents",
                )
                for document in input.data.documents
                if document.import_id != matching_document.import_id
            ]

            """
            Generate relationships

            This is mutation-y as we need the `transformed_related_documents` first, and using model_copy(update=dict) is not type safe.
            Setting the property directly is.
            """
            for related_document in transformed_related_documents:
                related_document.relationships = [
                    DocumentDocumentRelationship(
                        type="member_of",
                        document=DocumentWithoutRelationships(
                            **transformed_matching_document.model_dump()
                        ),
                    )
                ]

            transformed_matching_document.relationships = [
                DocumentDocumentRelationship(
                    type=next(
                        # search for a document with a role, or default to `has_member``
                        (
                            label.label.title.lower()
                            for label in related_document.labels
                            if label.type == "entity_type"
                        ),
                        "has_member",
                    ),
                    document=DocumentWithoutRelationships(
                        **related_document.model_dump()
                    ),
                )
                for related_document in transformed_related_documents
            ]

            return Success(
                [transformed_matching_document, *transformed_related_documents]
            )

    return Failure(
        CouldNotTransform(
            f"transform_navigator_family_with_single_matching_document could not transform {input.id}"
        )
    )


def transform_navigator_family_document(
    family: Identified[NavigatorFamily],
    document: NavigatorDocument,
    transformer_label_title: str,
) -> Document:
    # Labels
    labels: list[DocumentLabelRelationship] = []

    labels.append(
        DocumentLabelRelationship(
            type="family",
            label=Label(
                id=family.data.import_id, title=family.data.title, type="family"
            ),
        )
    )

    labels.append(
        DocumentLabelRelationship(
            type="transformer",
            label=TransformerLabel(
                id=transformer_label_title, title=transformer_label_title
            ),
        )
    )

    """
    These values are controlled
    @see: https://github.com/climatepolicyradar/data-migrations/blob/main/taxonomies/Intl.%20agreements.json#L42-L51
    """
    role = document.valid_metadata.get("role")
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

    return Document(id=document.import_id, title=document.title, labels=labels)


def transform_navigator_family_never(
    input: Identified[NavigatorFamily],
) -> Result[list[Document], CouldNotTransform]:
    related_documents = [
        transform_navigator_family_document(
            family=input,
            document=document,
            transformer_label_title="transform_navigator_family_never",
        )
        for document in input.data.documents
    ]
    return Success(
        [
            Document(
                id=input.data.import_id,
                title=input.data.title,
                labels=[
                    DocumentLabelRelationship(
                        type="family",
                        label=Label(
                            id=input.data.import_id,
                            title=input.data.title,
                            type="family",
                        ),
                    ),
                    DocumentLabelRelationship(
                        type="transformer",
                        label=TransformerLabel(
                            id="transform_navigator_family_never",
                            title="transform_navigator_family_never",
                            type="transformer",
                        ),
                    ),
                ],
            ),
            *related_documents,
        ]
    )


def transform_navigator_family_with_litigation_corpus_type_document(
    document: NavigatorDocument,
) -> Document:
    """
    These values are controlled
    @see: https://github.com/climatepolicyradar/data-migrations/blob/main/taxonomies/Litigation.json#L11-L113
    """
    labels: list[DocumentLabelRelationship] = []

    if document.events:
        event_type = document.events[0].event_type

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

    labels.append(
        DocumentLabelRelationship(
            type="transformer",
            label=TransformerLabel(
                id="transform_navigator_family_with_litigation_corpus_type",
                title="transform_navigator_family_with_litigation_corpus_type",
                type="transformer",
            ),
        )
    )

    return Document(id=document.import_id, title=document.title, labels=labels)


def transform_navigator_family_with_litigation_corpus_type(
    input: Identified[NavigatorFamily],
) -> Result[list[Document], CouldNotTransform]:
    if input.data.corpus.import_id == "Academic.corpus.Litigation.n0000":
        case_label = DocumentLabelRelationship(
            type="entity_type",
            label=Label(
                id="Legal case",
                title="Legal case",
                type="entity_type",
            ),
        )
        transformer_label = DocumentLabelRelationship(
            type="transformer",
            label=TransformerLabel(
                id="transform_navigator_family_with_litigation_corpus_type",
                title="transform_navigator_family_with_litigation_corpus_type",
                type="transformer",
            ),
        )
        document_from_family = Document(
            id=input.data.import_id,
            title=input.data.title,
            labels=[case_label, transformer_label],
        )
        related_documents = [
            transform_navigator_family_with_litigation_corpus_type_document(document)
            for document in input.data.documents
        ]

        """
        Generate relationships

        This is mutation-y as we need the `transformed_related_documents` first, and using model_copy(update=dict) is not type safe.
        Setting the property directly is.
        """
        for related_document in related_documents:
            related_document.relationships = [
                DocumentDocumentRelationship(
                    type="member_of",
                    document=DocumentWithoutRelationships(
                        **document_from_family.model_dump()
                    ),
                )
            ]

        document_from_family.relationships = [
            DocumentDocumentRelationship(
                type=next(
                    # search for a document with a `entity_type`, or default to `has_member`
                    (
                        label.label.title.lower()
                        for label in related_document.labels
                        if label.type == "entity_type"
                    ),
                    "has_member",
                ),
                document=DocumentWithoutRelationships(**related_document.model_dump()),
            )
            for related_document in related_documents
        ]

        return Success([document_from_family, *related_documents])

    return Failure(
        CouldNotTransform(
            f"transform_navigator_family_with_litigation_corpus_type could not transform {input.id}"
        )
    )
