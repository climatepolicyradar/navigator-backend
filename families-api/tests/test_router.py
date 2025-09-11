import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session

from app.router import APIListResponse, ConceptPublic


def test_returns_a_list_of_concepts_we_have_data_for(
    client: TestClient, session: Session, make_family
):
    id = 1
    (corpus_type, corpus, family, family_document, physical_document) = make_family(id)

    session.add(corpus_type)
    session.add(corpus)
    session.add(family)
    session.add(family_document)
    session.add(physical_document)

    session.commit()

    response = client.get("/families/concepts")

    assert response.status_code == 200
    response = APIListResponse[ConceptPublic].model_validate(response.json())
    assert response.data[0].id == "test concepts 1"
    assert response.data[1].id == "test concepts 2"


def test_does_not_include_duplicate_concepts_in_the_response(
    client: TestClient, session: Session, make_family
):
    id1 = 1
    (corpus_type1, corpus1, family1, _, _) = make_family(id1)
    # add another family with the same concepts
    id2 = 2
    (corpus_type2, corpus2, family2, _, _) = make_family(id2)

    session.add(corpus_type1)
    session.add(corpus_type2)
    session.add(corpus1)
    session.add(corpus2)
    session.add(family1)
    session.add(family2)

    session.commit()

    response = client.get("/families/concepts")

    assert response.status_code == 200
    response = APIListResponse[ConceptPublic].model_validate(response.json())
    assert len(response.data) == 2
    assert response.data[0].id == "test concepts 1"
    assert response.data[1].id == "test concepts 2"


@pytest.mark.parametrize("relation", [None, ""])
def test_does_not_include_concepts_with_no_relation(
    client: TestClient, session: Session, make_family, relation
):
    id = 1
    (corpus_type, corpus, family, _, _) = make_family(id)
    family.concepts = {
        "id": "test concepts 1",
        "ids": [],
        "type": "legal_entity",
        "relation": relation,
        "preferred_label": "test concept 1",
    }

    session.add(corpus_type)
    session.add(corpus)
    session.add(family)

    session.commit()

    response = client.get("/families/concepts")

    assert response.status_code == 200
    response = APIListResponse[ConceptPublic].model_validate(response.json())
    assert not response.data


@pytest.mark.parametrize("preferred_label", [None, ""])
def test_does_not_include_concepts_with_no_concept_preferred_label(
    client: TestClient, session: Session, make_family, preferred_label
):
    id = 1
    (corpus_type, corpus, family, _, _) = make_family(id)
    family.concepts = {
        "id": "test concepts 1",
        "ids": [],
        "type": "legal_entity",
        "relation": "category",
        "preferred_label": preferred_label,
    }

    session.add(corpus_type)
    session.add(corpus)
    session.add(family)

    session.commit()

    response = client.get("/families/concepts")

    assert response.status_code == 200
    response = APIListResponse[ConceptPublic].model_validate(response.json())
    assert not response.data


def test_returns_a_list_of_concepts_ordered_by_relation_and_then_preferred_label(
    client: TestClient, session: Session, make_family
):
    id = 1
    (corpus_type, corpus, family, family_document, physical_document) = make_family(id)
    family.concepts = (
        {
            "id": "test concepts 3",
            "ids": [],
            "type": "legal_entity",
            "relation": "jurisdiction",
            "preferred_label": "test concept 3",
        },
        {
            "id": "test concepts 1",
            "ids": [],
            "type": "legal_entity",
            "relation": "jurisdiction",
            "preferred_label": "test concept 1",
        },
        {
            "id": "test concepts 2",
            "ids": [],
            "type": "legal_entity",
            "relation": "category",
            "preferred_label": "test concept 2",
        },
    )

    session.add(corpus_type)
    session.add(corpus)
    session.add(family)
    session.add(family_document)
    session.add(physical_document)

    session.commit()

    response = client.get("/families/concepts")

    assert response.status_code == 200
    response = APIListResponse[ConceptPublic].model_validate(response.json())
    assert response.data[0].relation == "category"
    assert response.data[0].id == "test concepts 2"

    assert response.data[1].relation == "jurisdiction"
    assert response.data[1].id == "test concepts 1"

    assert response.data[2].relation == "jurisdiction"
    assert response.data[2].id == "test concepts 3"


def test_concepts_with_the_same_relation_and_preferred_label_are_included_in_the_response_if_subconcept_of_labels_is_different(
    client: TestClient, session: Session, make_family
):
    id = 1
    (corpus_type, corpus, family, _, _) = make_family(id)
    concept_base = {
        "id": "test concepts 1",
        "ids": [],
        "type": "legal_entity",
        "relation": "jurisdiction",
        "preferred_label": "test concept 1",
    }
    family.concepts = [
        {**concept_base, "subconcept_of_labels": ["parent concept 1"]},
        {**concept_base, "subconcept_of_labels": ["parent concept 2"]},
    ]

    session.add(corpus_type)
    session.add(corpus)
    session.add(family)

    session.commit()

    response = client.get("/families/concepts")

    assert response.status_code == 200
    response = APIListResponse[ConceptPublic].model_validate(response.json())
    assert len(response.data) == 2
    assert response.data[0].id == concept_base["id"]
    assert response.data[0].relation == concept_base["relation"]
    assert response.data[0].subconcept_of_labels == ["parent concept 1"]

    assert response.data[1].id == concept_base["id"]
    assert response.data[1].relation == concept_base["relation"]
    assert response.data[1].subconcept_of_labels == ["parent concept 2"]
