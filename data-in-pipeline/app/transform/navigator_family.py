from returns.result import Failure, Result, Success

from app.extract.connectors import NavigatorDocument, NavigatorFamily
from app.logging_config import get_logger
from app.models import Document, DocumentLabelRelationship, Identified, Label
from app.transform.models import CouldNotTransform, NoMatchingTransformations

_LOGGER = get_logger()


class TransformerLabel(Label):
    id: str
    title: str
    type: str = "transformer"


def transform_navigator_family(
    input: Identified[NavigatorFamily],
) -> Result[list[Document], NoMatchingTransformations]:
    transformers = [
        transform_navigator_family_with_single_matching_document,
        transform_navigator_family_with_matching_document_title_and_siblings,
        transform_navigator_family_never,
    ]

    success = None
    failures = []
    for transformer in transformers:
        result = transformer(input)
        match result:
            case Success(document):
                success = document
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


def transform_navigator_family_with_matching_document_title_and_siblings(
    input: Identified[NavigatorFamily],
) -> Result[list[Document], CouldNotTransform]:
    """
    We have some document <=> family relationships that are essentially just a
    repeat of each other.

    We can use this 1-1 mapping and put the data from the family and store on the document.

    We will also need to generate the relationships between the sibling documents.

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
            matching_transformation = transform_navigator_family_document(
                input,
                matching_document,
                "transform_navigator_family_with_matching_document_title_and_siblings",
            )
            siblings = [
                document
                for document in input.data.documents
                if document.import_id != matching_document.import_id
            ]
            transformed_siblings = [
                Document(id=document.import_id, title=document.title)
                for document in siblings
            ]

            return Success([matching_transformation, *transformed_siblings])

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
    transformer_label = TransformerLabel(
        id=transformer_label_title,
        title=transformer_label_title,
    )
    family_label = Label(
        id=family.data.import_id, title=family.data.title, type="family"
    )
    labels = [
        DocumentLabelRelationship(type="family", label=family_label),
        DocumentLabelRelationship(type="transformer", label=transformer_label),
    ]
    return Document(id=document.import_id, title=document.title, labels=labels)


def transform_navigator_family_never(
    input: Identified[NavigatorFamily],
) -> Result[Document, CouldNotTransform]:
    return Failure(
        CouldNotTransform(
            f"transform_navigator_family_never could not transform {input.id}"
        )
    )
