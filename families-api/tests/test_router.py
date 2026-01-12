from http import HTTPStatus

import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session

from app.models import FamilyPublic
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

    assert response.status_code == HTTPStatus.OK
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

    assert response.status_code == HTTPStatus.OK
    response = APIListResponse[ConceptPublic].model_validate(response.json())
    expected_number_of_concepts = 2
    assert len(response.data) == expected_number_of_concepts
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

    assert response.status_code == HTTPStatus.OK
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

    assert response.status_code == HTTPStatus.OK
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

    assert response.status_code == HTTPStatus.OK
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

    assert response.status_code == HTTPStatus.OK
    response = APIListResponse[ConceptPublic].model_validate(response.json())
    expected_number_of_concepts = 2
    assert len(response.data) == expected_number_of_concepts

    assert response.data[0].id == concept_base["id"]
    assert response.data[0].relation == concept_base["relation"]
    assert response.data[0].subconcept_of_labels == ["parent concept 1"]

    assert response.data[1].id == concept_base["id"]
    assert response.data[1].relation == concept_base["relation"]
    assert response.data[1].subconcept_of_labels == ["parent concept 2"]


def test_read_families_by_import_ids(client: TestClient, session: Session, make_family):
    """Test fetching families by import IDs list."""
    id1 = 1
    id2 = 2
    id3 = 3

    (corpus_type1, corpus1, family1, _, _) = make_family(id1)
    (corpus_type2, corpus2, family2, _, _) = make_family(id2)
    (corpus_type3, corpus3, family3, _, _) = make_family(id3)

    session.add(corpus_type1)
    session.add(corpus_type2)
    session.add(corpus_type3)
    session.add(corpus1)
    session.add(corpus2)
    session.add(corpus3)
    session.add(family1)
    session.add(family2)
    session.add(family3)
    session.commit()

    response = client.get("/families/?import_ids=family_1&import_ids=family_3")

    assert response.status_code == HTTPStatus.OK
    result = APIListResponse[FamilyPublic].model_validate(response.json())
    expected_number_of_families = 2
    assert len(result.data) == expected_number_of_families
    assert result.page == 1
    assert result.total == expected_number_of_families

    import_ids = [family.import_id for family in result.data]
    assert "family_1" in import_ids
    assert "family_3" in import_ids
    assert "family_2" not in import_ids


def test_read_families_by_import_ids_with_corpus_filter(
    client: TestClient, session: Session, make_family
):
    """Test fetching families by import IDs combined with corpus filter."""
    id1 = 1
    id2 = 2
    id3 = 3

    (corpus_type1, corpus1, family1, _, _) = make_family(id1)
    (corpus_type2, corpus2, family2, _, _) = make_family(id2)
    (corpus_type3, corpus3, family3, _, _) = make_family(id3)

    session.add(corpus_type1)
    session.add(corpus_type2)
    session.add(corpus_type3)
    session.add(corpus1)
    session.add(corpus2)
    session.add(corpus3)
    session.add(family1)
    session.add(family2)
    session.add(family3)
    session.commit()

    # Request family_1 and family_2, but filter by corpus_1
    # Only family_1 should be returned
    response = client.get(
        "/families/?import_ids=family_1&import_ids=family_2&corpus.import_id=corpus_1"
    )

    assert response.status_code == HTTPStatus.OK
    result = APIListResponse[FamilyPublic].model_validate(response.json())
    assert len(result.data) == 1
    assert result.data[0].import_id == "family_1"


def test_read_families_by_import_ids_empty_result(
    client: TestClient, session: Session, make_family
):
    """Test fetching families with non-existent import IDs."""
    id1 = 1
    (corpus_type1, corpus1, family1, _, _) = make_family(id1)

    session.add(corpus_type1)
    session.add(corpus1)
    session.add(family1)
    session.commit()

    response = client.get(
        "/families/?import_ids=nonexistent_1&import_ids=nonexistent_2"
    )

    assert response.status_code == HTTPStatus.OK
    result = APIListResponse[FamilyPublic].model_validate(response.json())
    assert len(result.data) == 0
    assert result.total == 0
