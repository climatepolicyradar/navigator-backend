from transformer.app.transformer import (
    Document,
    Label,
    NavigatorDocument,
    NavigatorFamily,
    transform,
)


def test_successfully_transforms_navigator_document_into_document():
    navigator_document = NavigatorDocument(
        id=7015,
        title="Law",
        family=NavigatorFamily(
            title="Federal Law No. 14.119/2021 on the National Policy for Payment for Environmental Services",
            summary="This law defines the concepts, objectives, guidelines, actions and criteria for implementing the National Policy for Payment for Environmental Services (PNPSA)",
            geographies=["BRA"],
        ),
    )

    single_document_in_family = Document(
        id=7015,
        title="Law",
        labels=[
            Label(
                type="family",
                title="Federal Law No. 14.119/2021 on the National Policy for Payment for Environmental Services",
            ),
            Label(type="geography", title="BRA"),
        ],
    )

    assert transform(navigator_document) == single_document_in_family
