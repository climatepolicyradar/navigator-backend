from data_in_models.models import (
    Document,
    DocumentRelationship,
    DocumentWithoutRelationships,
    Item,
    Label,
    LabelRelationship,
)

# @see: https://unfccc.int/resource/docs/convkp/conveng.pdf
unfccc_document = Document(
    id="__1",
    title="United Nations Framework Convention On Climate Change",
    labels=[
        LabelRelationship(
            type="debug",
            value=Label(
                type="debug",
                id="manual_input",
                value="manual_input",
            ),
        ),
        LabelRelationship(
            type="entity_type",
            value=Label(
                type="entity_type",
                id="Treaty",
                value="Treaty",
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
        LabelRelationship(
            type="debug",
            value=Label(
                type="debug",
                id="manual_input",
                value="manual_input",
            ),
        ),
        LabelRelationship(
            type="entity_type",
            value=Label(
                type="entity_type",
                id="Protocol",
                value="Protocol",
            ),
        ),
    ],
    documents=[
        DocumentRelationship(
            type="member_of",
            value=DocumentWithoutRelationships(**unfccc_document.model_dump()),
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
        LabelRelationship(
            type="debug",
            value=Label(
                type="debug",
                id="manual_input",
                value="manual_input",
            ),
        ),
        LabelRelationship(
            type="entity_type",
            value=Label(
                type="entity_type",
                id="Accord",
                value="Accord",
            ),
        ),
    ],
    documents=[
        DocumentRelationship(
            type="member_of",
            value=DocumentWithoutRelationships(**unfccc_document.model_dump()),
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
        LabelRelationship(
            type="debug",
            value=Label(
                type="debug",
                id="manual_input",
                value="manual_input",
            ),
        ),
        LabelRelationship(
            type="entity_type",
            value=Label(
                type="entity_type",
                id="Amendment",
                value="Amendment",
            ),
        ),
    ],
    documents=[
        DocumentRelationship(
            type="member_of",
            value=DocumentWithoutRelationships(**unfccc_document.model_dump()),
        ),
        DocumentRelationship(
            type="member_of",
            value=DocumentWithoutRelationships(
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
        LabelRelationship(
            type="debug",
            value=Label(
                type="debug",
                id="manual_input",
                value="manual_input",
            ),
        ),
        LabelRelationship(
            type="entity_type",
            value=Label(
                type="entity_type",
                id="Agreement",
                value="Agreement",
            ),
        ),
    ],
    documents=[
        DocumentRelationship(
            type="member_of",
            value=DocumentWithoutRelationships(**unfccc_document.model_dump()),
        )
    ],
    items=[
        Item(
            url="https://unfccc.int/sites/default/files/resource/parisagreement_publication.pdf",
        ),
    ],
)

unfccc_document.documents = [
    DocumentRelationship(
        type="has_member",
        value=DocumentWithoutRelationships(
            **unfccc_kyoto_protocol_document.model_dump()
        ),
    ),
    DocumentRelationship(
        type="has_member",
        value=DocumentWithoutRelationships(
            **unfccc_copenhagen_accord_document.model_dump()
        ),
    ),
    DocumentRelationship(
        type="has_member",
        value=DocumentWithoutRelationships(
            **unfccc_kyoto_protocol_doha_amendment_document.model_dump()
        ),
    ),
    DocumentRelationship(
        type="has_member",
        value=DocumentWithoutRelationships(
            **unfccc_paris_agreement_document.model_dump()
        ),
    ),
]
