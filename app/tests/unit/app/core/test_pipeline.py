import json
from typing import Dict
from unittest.mock import patch

from click.testing import CliRunner
from tests.non_search.setup_helpers import (
    setup_docs_with_two_orgs,
    setup_with_documents_large_with_families,
    setup_with_two_docs_multiple_languages,
    setup_with_two_docs_one_family,
    setup_with_two_unpublished_docs,
)

from app.repository.pipeline import generate_pipeline_ingest_input
from app.service.pipeline import format_pipeline_ingest_input, get_db_state_content
from scripts.db_state_validator_click import main as db_state_validator_main


def test_generate_pipeline_ingest_input(data_db):
    setup_with_two_docs_one_family(data_db)

    state_rows = generate_pipeline_ingest_input(data_db)
    assert len(state_rows) == 2
    # Sort to ensure order is consistent across tests
    state_rows = sorted(state_rows, key=lambda d: d.import_id, reverse=True)

    # Now test one field from each table we've queried
    # Check family title
    assert state_rows[0].name == "Fam1"
    assert state_rows[1].name == "Fam1"

    # Check family_document import_id
    assert state_rows[0].import_id == "CCLW.executive.2.2"
    assert state_rows[1].import_id == "CCLW.executive.1.2"

    # Check family_metadata
    assert state_rows[0].metadata["family.size"] == "big"

    # Check geography
    assert state_rows[0].geographies == ["South Asia"]

    # Check organisation
    assert state_rows[0].source == "CCLW"

    # Check corpus
    assert state_rows[0].corpus_import_id == "CCLW.corpus.i00000001.n0000"

    # Check physical_document
    assert state_rows[0].document_title == "Document2"


def test_generate_pipeline_ingest_input_with_fixture(
    documents_large: list[Dict], data_db
):
    setup_with_documents_large_with_families(documents_large, data_db)

    state_rows = generate_pipeline_ingest_input(data_db)

    assert len(state_rows) == 23


def test_generate_pipeline_ingest_input_no_collection_family_link(data_db):
    setup_docs_with_two_orgs(data_db)

    state_rows = generate_pipeline_ingest_input(data_db)
    assert len(state_rows) == 2


def test_generate_pipeline_ingest_input__deleted(data_db):
    setup_with_two_unpublished_docs(data_db)

    documents = generate_pipeline_ingest_input(data_db)
    assert len(documents) == 1
    assert documents[0].name == "Fam1"
    assert documents[0].import_id == "CCLW.executive.1.2"


def test_get_db_state_content_success(data_db, caplog):
    """
    GIVEN an expected db state file
    WHEN the branch db state content is identical (bar ordering)
    THEN the db state validator should succeed
    """
    setup_with_two_docs_multiple_languages(data_db)

    expected_db_state_contents = {
        "documents": {
            "CCLW.executive.1.2": {
                "name": "Fam1",
                "document_title": "Document1",
                "description": "Summary1",
                "import_id": "CCLW.executive.1.2",
                "slug": "DocSlug1",
                "family_import_id": "CCLW.family.1001.0",
                "family_slug": "FamSlug1",
                "publication_ts": "2019-12-25T00:00:00+00:00",
                "date": None,
                "source_url": "http://somewhere1",
                "download_url": None,
                "corpus_import_id": "CCLW.corpus.i00000001.n0000",
                "corpus_type_name": "Laws and Policies",
                "collection_title": None,
                "collection_summary": None,
                "type": "Plan",
                "source": "CCLW",
                "category": "Executive",
                "geography": "South Asia",
                "geographies": ["South Asia"],
                "languages": ["French", "English"],
                "metadata": {
                    "family.size": "big",
                    "family.color": "pink",
                    "document.role": ["MAIN"],
                    "document.type": ["Plan"],
                },
            },
            "CCLW.executive.2.2": {
                "name": "Fam2",
                "document_title": "Document2",
                "description": "Summary2",
                "import_id": "CCLW.executive.2.2",
                "slug": "DocSlug2",
                "family_import_id": "CCLW.family.2002.0",
                "family_slug": "FamSlug2",
                "publication_ts": "2019-12-25T00:00:00+00:00",
                "date": None,
                "source_url": "http://another_somewhere",
                "download_url": None,
                "corpus_import_id": "CCLW.corpus.i00000001.n0000",
                "corpus_type_name": "Laws and Policies",
                "collection_title": None,
                "collection_summary": None,
                "type": "Order",
                "source": "CCLW",
                "category": "Executive",
                "geography": "AFG",
                "geographies": ["AFG", "IND"],
                "languages": [],
                "metadata": {
                    "family.size": "small",
                    "family.color": "blue",
                    "document.role": ["MAIN"],
                    "document.type": ["Order"],
                },
            },
        }
    }

    actual_db_state_content = get_db_state_content(data_db)

    # Use the db_state_validator to verify the db_state files are alike.
    runner = CliRunner()
    with runner.isolated_filesystem():
        with open("expected.json", "w") as f:
            f.write(json.dumps(expected_db_state_contents))

        with open("actual.json", "w") as f:
            f.write(json.dumps(actual_db_state_content))

        result = runner.invoke(
            db_state_validator_main, ["expected.json", "actual.json"]
        )

    assert result.exit_code == 0
    assert "🎉 DB states are equivalent!" in caplog.messages


@patch("json.dump")
def test_get_db_state_content_fails_when_mismatch(mock_json_dump, data_db, caplog):
    """
    GIVEN a db state file where the doc CCLW.executive.1.2 has 2 langs
    WHEN the branch db state contains a third lang for this document
    THEN fail the db state validator & output the differences
    """
    setup_with_two_docs_multiple_languages(data_db)

    expected_main_db_state = generate_pipeline_ingest_input(data_db)

    modified_branch_db_state = {
        "documents": {
            "CCLW.executive.1.2": {
                "name": "Fam1",
                "document_title": "Document1",
                "description": "Summary1",
                "import_id": "CCLW.executive.1.2",
                "slug": "DocSlug1",
                "family_import_id": "CCLW.family.1001.0",
                "family_slug": "FamSlug1",
                "publication_ts": "2019-12-25T00:00:00+00:00",
                "date": None,
                "source_url": "http://somewhere1",
                "download_url": None,
                "corpus_import_id": "CCLW.corpus.i00000001.n0000",
                "corpus_type_name": "Laws and Policies",
                "collection_title": None,
                "collection_summary": None,
                "type": "Plan",
                "source": "CCLW",
                "category": "Executive",
                "geography": "South Asia",
                "geographies": ["South Asia"],
                "languages": ["French", "English", "NewLanguage"],
                "metadata": {
                    "family.size": "big",
                    "family.color": "pink",
                    "document.role": ["MAIN"],
                    "document.type": ["Plan"],
                },
            },
            "CCLW.executive.2.2": {
                "name": "Fam2",
                "document_title": "Document2",
                "description": "Summary2",
                "import_id": "CCLW.executive.2.2",
                "slug": "DocSlug2",
                "family_import_id": "CCLW.family.2002.0",
                "family_slug": "FamSlug2",
                "publication_ts": "2019-12-25T00:00:00+00:00",
                "date": None,
                "source_url": "http://another_somewhere",
                "download_url": None,
                "corpus_import_id": "CCLW.corpus.i00000001.n0000",
                "corpus_type_name": "Laws and Policies",
                "collection_title": None,
                "collection_summary": None,
                "type": "Order",
                "source": "CCLW",
                "category": "Executive",
                "geography": "AFG",
                "geographies": ["AFG", "IND"],
                "languages": [],
                "metadata": {
                    "family.size": "small",
                    "family.color": "blue",
                    "document.role": ["MAIN"],
                    "document.type": ["Order"],
                },
            },
        }
    }

    # Use the db_state_validator to verify the db_state files are alike.
    runner = CliRunner()
    with runner.isolated_filesystem():
        with open("expected.json", "w") as f:
            f.write(json.dumps(format_pipeline_ingest_input(expected_main_db_state)))

        with open("actual.json", "w") as f:
            f.write(json.dumps(modified_branch_db_state))

        result = runner.invoke(
            db_state_validator_main, ["expected.json", "actual.json"]
        )

    assert result.exit_code == 1
    assert "Field 'languages' differs" in caplog.text

    # Check the JSON differences were written
    mock_json_dump.assert_called_once()
    differences = mock_json_dump.call_args[0][0]
    assert "differences" in differences

    assert "CCLW.executive.1.2" in differences["differences"]
    assert "languages" in differences["differences"]["CCLW.executive.1.2"]
    assert all(
        k in differences["differences"]["CCLW.executive.1.2"]["languages"]
        for k in ["main", "branch"]
    )
    assert ["English", "French"] == differences["differences"]["CCLW.executive.1.2"][
        "languages"
    ]["main"]
    assert ["English", "French", "NewLanguage"] == differences["differences"][
        "CCLW.executive.1.2"
    ]["languages"]["branch"]
