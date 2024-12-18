import os
from datetime import datetime
from http.client import OK
from typing import Any
from unittest.mock import MagicMock

import jwt
import pytest
from dateutil.relativedelta import relativedelta
from db_client.models.dfce.family import (
    Family,
    FamilyCategory,
    FamilyCorpus,
    FamilyGeography,
)
from db_client.models.organisation import Corpus, CorpusType, Organisation

from app.clients.db.session import SessionLocal
from app.service import security
from app.service.util import tree_table_to_json

LEN_ORG_CONFIG = 3
EXPECTED_CCLW_TAXONOMY = {
    "instrument",
    "keyword",
    "sector",
    "topic",
    "framework",
    "hazard",
    "_document",
    "_event",
    "event_type",
}
EXPECTED_CCLW_EVENTS = [
    "Amended",
    "Appealed",
    "Closed",
    "Declaration Of Climate Emergency",
    "Dismissed",
    "Entered Into Force",
    "Filing",
    "Granted",
    "Implementation Details",
    "International Agreement",
    "Net Zero Pledge",
    "Other",
    "Passed/Approved",
    "Repealed/Replaced",
    "Set",
    "Settled",
    "Updated",
]


EXPECTED_UNFCCC_TAXONOMY = {
    "author",
    "author_type",
    "event_type",
    "_document",
    "_event",
}


def _add_family(test_db, import_id: str, cat: FamilyCategory, corpus_import_id):
    test_db.add(
        Family(
            title="f1",
            import_id=import_id,
            description="",
            family_category=cat,
        )
    )
    test_db.add(FamilyGeography(family_import_id=import_id, geography_id=1))
    test_db.add(
        FamilyCorpus(family_import_id=import_id, corpus_import_id=corpus_import_id)
    )


def test_config_endpoint_content(data_client, data_db, app_token_factory, valid_token):
    """Tests whether we get the expected content when the /config endpoint is called."""
    # TODO: this test is fragile, we should look into validation according to the
    #       supporting data, rather than counts & fixed lists
    url_under_test = "/api/v1/config"
    app_token = app_token_factory(
        "CCLW.corpus.i00000001.n0000,UNFCCC.corpus.i00000001.n0000"
    )

    response = data_client.get(url_under_test, headers={"app-token": app_token})

    response_json = response.json()

    assert response.status_code == OK
    assert set(response_json.keys()) == {
        "geographies",
        "organisations",
        "document_variants",
        "languages",
        "corpus_types",
    }

    assert "geographies" in response_json
    assert len(response_json["geographies"]) == 8

    assert "languages" in response_json
    assert len(response_json["languages"]) == 7893

    assert "fra" in response_json["languages"]
    assert all(len(key) == 3 for key in response_json["languages"])

    assert "document_variants" in response_json
    assert len(response_json["document_variants"]) == 2
    assert "Original Language" in response_json["document_variants"]

    corpus_types = response_json["corpus_types"]
    assert list(corpus_types.keys()) == ["Laws and Policies", "Intl. agreements"]

    laws_and_policies = corpus_types["Laws and Policies"]
    assert laws_and_policies["corpus_type_description"] == "Laws and policies"

    taxonomy = laws_and_policies["taxonomy"]
    assert set(taxonomy) ^ EXPECTED_CCLW_TAXONOMY == set()
    # Check document roles.
    assert "role" in taxonomy["_document"].keys()
    assert len(taxonomy["_document"]["role"]["allowed_values"]) == 10
    assert "MAIN" in taxonomy["_document"]["role"]["allowed_values"]
    # Check document roles.
    assert "type" in taxonomy["_document"].keys()
    assert len(taxonomy["_document"]["type"]["allowed_values"]) == 76
    assert "Adaptation Communication" in taxonomy["_document"]["type"]["allowed_values"]
    # Check event types.
    assert len(taxonomy["_event"]["event_type"]["allowed_values"]) == 17
    assert "Passed/Approved" in taxonomy["_event"]["event_type"]["allowed_values"]

    assert len(laws_and_policies["corpora"]) == 1
    cclw_corpus = laws_and_policies["corpora"][0]

    assert cclw_corpus["total"] == 0
    assert cclw_corpus["count_by_category"] == {
        "Executive": 0,
        "Legislative": 0,
        "UNFCCC": 0,
        "MCF": 0,
    }

    assert cclw_corpus["corpus_import_id"] == "CCLW.corpus.i00000001.n0000"
    assert cclw_corpus["organisation_name"] == "CCLW"
    assert cclw_corpus["organisation_id"] == 1
    assert (
        cclw_corpus["image_url"]
        == "https://cdn.climatepolicyradar.org/corpora/CCLW.corpus.i00000001.n0000/logo.png"
    )
    assert "Grantham Research Institute" in cclw_corpus["text"]
    assert cclw_corpus["description"] == "CCLW national policies"
    assert cclw_corpus["title"] == "CCLW national policies"

    # Below to be removed as part of PDCT-1759
    # Now test organisations
    assert "CCLW" in response_json["organisations"]
    cclw_org = response_json["organisations"]["CCLW"]
    assert len(cclw_org) == LEN_ORG_CONFIG

    # Test the counts are there (just CCLW)
    assert cclw_org["total"] == 0
    assert cclw_org["count_by_category"] == {
        "Executive": 0,
        "Legislative": 0,
        "UNFCCC": 0,
        "MCF": 0,
    }

    assert "UNFCCC" in response_json["organisations"]
    unfccc_org = response_json["organisations"]["UNFCCC"]
    assert len(unfccc_org) == LEN_ORG_CONFIG

    cclw_corpora = cclw_org["corpora"]
    assert len(cclw_corpora) == 1
    assert cclw_corpora[0]["corpus_import_id"] == "CCLW.corpus.i00000001.n0000"
    assert cclw_corpora[0]["corpus_type"] == "Laws and Policies"
    assert (
        cclw_corpora[0]["image_url"]
        == "https://cdn.climatepolicyradar.org/corpora/CCLW.corpus.i00000001.n0000/logo.png"
    )
    assert "Grantham Research Institute" in cclw_corpora[0]["text"]
    assert cclw_corpora[0]["corpus_type_description"] == "Laws and policies"
    assert cclw_corpora[0]["description"] == "CCLW national policies"
    assert cclw_corpora[0]["title"] == "CCLW national policies"
    assert set(cclw_corpora[0]["taxonomy"]) ^ EXPECTED_CCLW_TAXONOMY == set()

    # Check document roles.
    assert "role" in cclw_corpora[0]["taxonomy"]["_document"].keys()
    assert len(cclw_corpora[0]["taxonomy"]["_document"]["role"]["allowed_values"]) == 10
    assert "MAIN" in cclw_corpora[0]["taxonomy"]["_document"]["role"]["allowed_values"]

    # Check document roles.
    assert "type" in cclw_corpora[0]["taxonomy"]["_document"].keys()
    assert len(cclw_corpora[0]["taxonomy"]["_document"]["type"]["allowed_values"]) == 76
    assert (
        "Adaptation Communication"
        in cclw_corpora[0]["taxonomy"]["_document"]["type"]["allowed_values"]
    )

    # Check event types.
    assert (
        len(cclw_corpora[0]["taxonomy"]["_event"]["event_type"]["allowed_values"]) == 17
    )
    assert (
        "Passed/Approved"
        in cclw_corpora[0]["taxonomy"]["_event"]["event_type"]["allowed_values"]
    )


def test_config_endpoint_cclw_stats(data_client, data_db, valid_token):
    url_under_test = "/api/v1/config"

    cclw = (
        data_db.query(Corpus)
        .join(Organisation, Organisation.id == Corpus.organisation_id)
        .filter(Organisation.name == "CCLW")
        .one()
    )
    unfccc = (
        data_db.query(Corpus)
        .join(Organisation, Organisation.id == Corpus.organisation_id)
        .filter(Organisation.name == "UNFCCC")
        .one()
    )
    unfccc = data_db.query(Corpus).filter(Corpus.organisation_id == 1).one()

    # Add some data here
    _add_family(data_db, "T.0.0.1", FamilyCategory.EXECUTIVE, cclw.import_id)
    _add_family(data_db, "T.0.0.2", FamilyCategory.EXECUTIVE, cclw.import_id)
    _add_family(data_db, "T.0.0.3", FamilyCategory.EXECUTIVE, cclw.import_id)
    _add_family(data_db, "T.0.0.4", FamilyCategory.LEGISLATIVE, cclw.import_id)
    _add_family(data_db, "T.0.0.5", FamilyCategory.LEGISLATIVE, cclw.import_id)
    _add_family(data_db, "T.0.0.6", FamilyCategory.UNFCCC, unfccc.import_id)
    data_db.flush()

    response = data_client.get(url_under_test, headers={"app-token": valid_token})

    response_json = response.json()

    corpora = response_json["corpora"]
    assert len(corpora) == 2
    cclw_corpus_config = next(
        (corpus for corpus in corpora if "CCLW" in corpus["corpus_import_id"]), {}
    )
    laws = cclw_corpus_config["count_by_category"]["Legislative"]
    policies = cclw_corpus_config["count_by_category"]["Executive"]
    unfccc = cclw_corpus_config["count_by_category"]["UNFCCC"]
    assert laws == 2
    assert policies == 3
    assert unfccc == 1

    assert cclw_corpus_config["total"] == laws + policies + unfccc

    # Below to be removed as part of PDCT-1759
    org_config = response_json["organisations"]["CCLW"]
    assert len(org_config) == LEN_ORG_CONFIG
    assert org_config["total"] == 6

    laws = org_config["count_by_category"]["Legislative"]
    policies = org_config["count_by_category"]["Executive"]
    unfccc = org_config["count_by_category"]["UNFCCC"]
    assert laws == 2
    assert policies == 3
    assert unfccc == 1

    assert org_config["total"] == laws + policies + unfccc


@pytest.mark.parametrize(
    "allowed_corpora_ids, expected_organisation, other_organisation",
    [
        ("UNFCCC.corpus.i00000001.n0000", "UNFCCC", "CCLW"),
        ("CCLW.corpus.i00000001.n0000", "CCLW", "UNFCCC"),
    ],
)
def test_config_endpoint_returns_stats_for_allowed_corpora_only(
    allowed_corpora_ids,
    expected_organisation,
    other_organisation,
    app_token_factory,
    data_client,
    data_db,
):
    app_token = app_token_factory(allowed_corpora_ids)
    url_under_test = "/api/v1/config"

    other_corpus = (
        data_db.query(Corpus)
        .join(Organisation, Organisation.id == Corpus.organisation_id)
        .filter(Organisation.name == other_organisation)
        .one()
    )
    expected_corpus = (
        data_db.query(Corpus)
        .join(Organisation, Organisation.id == Corpus.organisation_id)
        .filter(Organisation.name == expected_organisation)
        .one()
    )
    expected_corpus_type = (
        data_db.query(CorpusType)
        .join(Corpus, Corpus.corpus_type_name == CorpusType.name)
        .filter(CorpusType.name == expected_corpus.corpus_type_name)
        .one()
    )

    _add_family(data_db, "T.0.0.1", FamilyCategory.EXECUTIVE, other_corpus.import_id)
    _add_family(
        data_db, "T.0.0.2", FamilyCategory.LEGISLATIVE, expected_corpus.import_id
    )
    data_db.flush()

    response = data_client.get(url_under_test, headers={"app-token": app_token})

    response_json = response.json()

    assert len(response_json["corpora"]) == 1
    corpus_config = response_json["corpora"][0]
    assert corpus_config["total"] == 1
    assert corpus_config["count_by_category"] == {
        "Executive": 0,
        "Legislative": 1,
        "MCF": 0,
        "UNFCCC": 0,
    }

    #  Below to be removed as part of PDCT-1759
    org_config = response_json["organisations"]
    expected_org_config = {
        expected_organisation: {
            "corpora": [
                {
                    "corpus_import_id": expected_corpus.import_id,
                    "title": expected_corpus.title,
                    "description": expected_corpus.description,
                    "corpus_type": expected_corpus.corpus_type_name,
                    "corpus_type_description": expected_corpus_type.description,
                    "taxonomy": expected_corpus_type.valid_metadata,
                    "text": expected_corpus.corpus_text,
                    "image_url": (
                        f"https://cdn.climatepolicyradar.org/{expected_corpus.corpus_image_url}"
                        if expected_corpus.corpus_image_url
                        else ""
                    ),
                }
            ],
            "total": 1,
            "count_by_category": {
                "Executive": 0,
                "Legislative": 1,
                "MCF": 0,
                "UNFCCC": 0,
            },
        },
    }
    assert org_config == expected_org_config


def test_config_endpoint_returns_stats_for_all_orgs_if_no_allowed_corpora_in_app_token(
    data_client,
    data_db,
):
    issued_at = datetime.utcnow()
    to_encode = {
        "allowed_corpora_ids": [],
        "exp": issued_at + relativedelta(years=10),
        "iat": int(datetime.timestamp(issued_at.replace(microsecond=0))),
        "iss": "Climate Policy Radar",
        "sub": "CPR",
        "aud": "localhost",
    }
    app_token = jwt.encode(
        to_encode, os.environ["TOKEN_SECRET_KEY"], algorithm=security.ALGORITHM
    )
    url_under_test = "/api/v1/config"

    cclw_corpus = (
        data_db.query(Corpus)
        .join(Organisation, Organisation.id == Corpus.organisation_id)
        .filter(Organisation.name == "CCLW")
        .one()
    )

    unfccc_corpus = (
        data_db.query(Corpus)
        .join(Organisation, Organisation.id == Corpus.organisation_id)
        .filter(Organisation.name == "UNFCCC")
        .one()
    )

    _add_family(data_db, "T.0.0.1", FamilyCategory.EXECUTIVE, cclw_corpus.import_id)
    _add_family(data_db, "T.0.0.2", FamilyCategory.EXECUTIVE, unfccc_corpus.import_id)
    data_db.flush()

    response = data_client.get(url_under_test, headers={"app-token": app_token})

    response_json = response.json()

    assert len(response_json["corpora"]) == 2
    corpora = response_json["corpora"]
    for corpus in corpora:
        assert corpus["total"] == 1
        assert corpus["count_by_category"] == {
            "Executive": 1,
            "Legislative": 0,
            "MCF": 0,
            "UNFCCC": 0,
        }

    #  Below to be removed as part of PDCT-1759
    org_config = response_json["organisations"]

    assert list(org_config.keys()) == ["CCLW", "UNFCCC"]
    assert org_config["CCLW"]["total"] == 1
    assert org_config["UNFCCC"]["total"] == 1
    assert org_config["UNFCCC"]["count_by_category"] == {
        "Executive": 1,
        "Legislative": 0,
        "MCF": 0,
        "UNFCCC": 0,
    }
    assert org_config["CCLW"]["count_by_category"] == {
        "Executive": 1,
        "Legislative": 0,
        "MCF": 0,
        "UNFCCC": 0,
    }


class _MockColumn:
    def __init__(self, name):
        self.name = name


class _MockTable:
    def __init__(self, columns: list[str]):
        self.columns = [_MockColumn(c) for c in columns]


class _MockRow:
    def __init__(self, data: dict[str, Any]):
        self.__table__ = _MockTable(list(data.keys()))
        for key, value in data.items():
            setattr(self, key, value)


class _MockQuery:
    def __init__(self, query_response_data):
        self.query_response_data = query_response_data

    def all(self):
        return [_MockRow(rd) for rd in self.query_response_data]


_DATA_1 = {"id": 1, "parent_id": None, "name": "root", "data": 1}
_DATA_2 = {"id": 2, "parent_id": 1, "name": "two", "data": 2}
_DATA_3 = {"id": 3, "parent_id": 2, "name": "three", "data": 3}
TREE_TABLE_DATA_1 = [_DATA_1, _DATA_2, _DATA_3]
_EX_THREE = {"node": _DATA_3, "children": []}
_EX_TWO = {"node": _DATA_2, "children": [_EX_THREE]}
_EX_ONE = {"node": _DATA_1, "children": [_EX_TWO]}
EXPECTED_TREE_1 = [_EX_ONE]


@pytest.mark.parametrize("data,expected", [(TREE_TABLE_DATA_1, EXPECTED_TREE_1)])
def test_tree_table_to_json(data, expected):
    db = MagicMock(spec=SessionLocal)
    db_query_mock = MagicMock()
    db_query_mock.order_by = lambda _: _MockQuery(data)
    db.query = lambda _: db_query_mock

    table_mock = MagicMock()
    table_mock.id = 1
    processed_data = tree_table_to_json(table_mock, db)

    assert processed_data == expected
