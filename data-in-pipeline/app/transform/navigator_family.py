from app.extract.connectors import NavigatorFamily
from returns.result import Failure, Result, Success

from app.extract.connectors import NavigatorFamily
from app.models import Document, Identified
from app.transform.models import CouldNotTransform, NoMatchingTransformations


def transform_navigator_family(
    input: Identified[NavigatorFamily],
) -> Result[Document, NoMatchingTransformations]:
    transformers = [
        transform_navigator_family_with_single_matching_document,
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
) -> Result[Document, CouldNotTransform]:
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
        return Success(Document(id=input.id, title=input.data.title))
    else:
        return Failure(
            CouldNotTransform(
                f"transform_navigator_family_with_single_matching_document could not transform {input.id}"
            )
        )


def transform_navigator_family_never(
    input: Identified[NavigatorFamily],
) -> Result[Document, CouldNotTransform]:
    return Failure(
        CouldNotTransform(
            f"transform_navigator_family_never could not transform {input.id}"
        )
    )
