from app.models import (
    Document,
    DocumentDocumentRelationship,
    DocumentLabelRelationship,
    DocumentWithoutRelationships,
    Item,
    Label,
)

# @see: https://unfccc.int/resource/docs/convkp/conveng.pdf
unfccc_document = Document(
    id="__1",
    title="United Nations Framework Convention On Climate Change",
    labels=[
        DocumentLabelRelationship(
            type="debug",
            label=Label(
                type="debug",
                id="manual_input",
                title="manual_input",
            ),
        ),
        DocumentLabelRelationship(
            type="entity_type",
            label=Label(
                type="entity_type",
                id="Treaty",
                title="Treaty",
            ),
        ),
    ],
    items=[
        Item(
            url="https://unfccc.int/resource/docs/convkp/conveng.pdf",
        ),
    ],
)

# @see: https://unfccc.int/resource/docs/convkp/kpeng.pdf
unfccc_kyoto_protocol_document = Document(
    id="__2",
    title="Kyoto Protocol To The United Nations Framework Convention On Climate Change",
    labels=[
        DocumentLabelRelationship(
            type="debug",
            label=Label(
                type="debug",
                id="manual_input",
                title="manual_input",
            ),
        ),
        DocumentLabelRelationship(
            type="entity_type",
            label=Label(
                type="entity_type",
                id="Protocol",
                title="Protocol",
            ),
        ),
    ],
    relationships=[
        DocumentDocumentRelationship(
            type="member_of",
            document=DocumentWithoutRelationships(**unfccc_document.model_dump()),
        )
    ],
    items=[
        Item(
            url="https://unfccc.int/resource/docs/convkp/kpeng.pdf",
        ),
    ],
)

# @see: https://unfccc.int/resource/docs/2009/cop15/eng/11a01.pdf
unfccc_copenhagen_accord_document = Document(
    id="__3",
    title="The Copenhagen Accord",
    labels=[
        DocumentLabelRelationship(
            type="debug",
            label=Label(
                type="debug",
                id="manual_input",
                title="manual_input",
            ),
        ),
        DocumentLabelRelationship(
            type="entity_type",
            label=Label(
                type="entity_type",
                id="Accord",
                title="Accord",
            ),
        ),
    ],
    relationships=[
        DocumentDocumentRelationship(
            type="member_of",
            document=DocumentWithoutRelationships(**unfccc_document.model_dump()),
        )
    ],
    items=[
        Item(
            url="https://unfccc.int/resource/docs/2009/cop15/eng/11a01.pdf",
        ),
    ],
)

# @see: https://unfccc.int/files/kyoto_protocol/application/pdf/kp_doha_amendment_english.pdf
unfccc_kyoto_protocol_doha_amendment_document = Document(
    id="__4",
    title="Doha amendment to the Kyoto Protocol",
    labels=[
        DocumentLabelRelationship(
            type="debug",
            label=Label(
                type="debug",
                id="manual_input",
                title="manual_input",
            ),
        ),
        DocumentLabelRelationship(
            type="entity_type",
            label=Label(
                type="entity_type",
                id="Amendment",
                title="Amendment",
            ),
        ),
    ],
    relationships=[
        DocumentDocumentRelationship(
            type="member_of",
            document=DocumentWithoutRelationships(**unfccc_document.model_dump()),
        ),
        DocumentDocumentRelationship(
            type="member_of",
            document=DocumentWithoutRelationships(
                **unfccc_kyoto_protocol_document.model_dump()
            ),
        ),
    ],
    items=[
        Item(
            url="https://unfccc.int/files/kyoto_protocol/application/pdf/kp_doha_amendment_english.pdf",
        ),
    ],
)

# @see: https://unfccc.int/sites/default/files/resource/parisagreement_publication.pdf
unfccc_paris_agreement_document = Document(
    id="__5",
    title="The Paris Agreement",
    labels=[
        DocumentLabelRelationship(
            type="debug",
            label=Label(
                type="debug",
                id="manual_input",
                title="manual_input",
            ),
        ),
        DocumentLabelRelationship(
            type="entity_type",
            label=Label(
                type="entity_type",
                id="Agreement",
                title="Agreement",
            ),
        ),
    ],
    relationships=[
        DocumentDocumentRelationship(
            type="member_of",
            document=DocumentWithoutRelationships(**unfccc_document.model_dump()),
        )
    ],
    items=[
        Item(
            url="https://unfccc.int/sites/default/files/resource/parisagreement_publication.pdf",
        ),
    ],
)

unfccc_document.relationships = [
    DocumentDocumentRelationship(
        type="has_member",
        document=DocumentWithoutRelationships(
            **unfccc_kyoto_protocol_document.model_dump()
        ),
    ),
    DocumentDocumentRelationship(
        type="has_member",
        document=DocumentWithoutRelationships(
            **unfccc_copenhagen_accord_document.model_dump()
        ),
    ),
    DocumentDocumentRelationship(
        type="has_member",
        document=DocumentWithoutRelationships(
            **unfccc_kyoto_protocol_doha_amendment_document.model_dump()
        ),
    ),
    DocumentDocumentRelationship(
        type="has_member",
        document=DocumentWithoutRelationships(
            **unfccc_paris_agreement_document.model_dump()
        ),
    ),
]
