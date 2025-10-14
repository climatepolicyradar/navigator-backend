from app.transformer.main import (
    Document,
    DocumentLabelRelationship,
    Label,
    NavigatorDocument,
    NavigatorFamily,
    NavigatorGeography,
    transform,
)


def test_successfully_transforms_navigator_document_into_document():
    navigator_document = NavigatorDocument(
        id=7015,
        title="Law",
        family=NavigatorFamily(
            title="Federal Law No. 14.119/2021 on the National Policy for Payment for Environmental Services",
            summary="This law defines the concepts, objectives, guidelines, actions and criteria for implementing the National Policy for Payment for Environmental Services (PNPSA)",
            geographies=[
                NavigatorGeography(
                    id=1,
                    display_value="Brazil",
                    value="BRA",
                    type="ISO-3166",
                    parent_id=None,
                    slug="brazil",
                ),
            ],
        ),
    )

    single_document_in_family = Document(
        id=7015,
        title="Law",
        labels=[
            DocumentLabelRelationship(
                label=Label(
                    type="family",
                    title="Federal Law No. 14.119/2021 on the National Policy for Payment for Environmental Services",
                ),
                relationship="part_of",
            ),
            DocumentLabelRelationship(
                label=Label(type="geography", title="Brazil"),
                relationship="part_of",
            ),
        ],
    )

    assert transform(navigator_document) == single_document_in_family
