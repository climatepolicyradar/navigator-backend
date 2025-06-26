import json
import logging
import sys
from datetime import datetime
from typing import List, cast

import click
from cpr_sdk.parser_models import BackendDocument
from cpr_sdk.pipeline_general_models import InputData
from pydantic import ValidationError

# Setup logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_json(file_path: str) -> InputData:
    """Load JSON from a file and validate it against the DBState model.

    :param file_path: Path to the JSON file
    :type file_path: str
    :raises SystemExit: If there's an error loading/validating the JSON
    :return: The validated database state
    :rtype: DBState
    """
    try:
        with open(file_path, "r") as file:
            data = json.load(file)
        return InputData(**data)
    except (json.JSONDecodeError, ValidationError) as e:
        logger.error(f"💥 Error loading JSON from {file_path}: {e}")
        sys.exit(1)


def find_differing_doc_import_ids(
    main_sorted: List[BackendDocument], branch_sorted: List[BackendDocument]
) -> bool:
    main_set = {doc.import_id for doc in main_sorted}
    branch_set = {doc.import_id for doc in branch_sorted}
    missing_in_branch = main_set - branch_set
    extra_in_branch = branch_set - main_set

    if missing_in_branch or extra_in_branch:
        if missing_in_branch:
            logger.info(f"🔍 Missing doc IDs in branch: {missing_in_branch}")
        if extra_in_branch:
            logger.info(
                f"🔍 Extra doc IDs in branch compared with main: {extra_in_branch}"
            )
        return True
    return False


def find_document_differences(
    main_sorted: List[BackendDocument], branch_sorted: List[BackendDocument]
) -> bool:
    """Compare each document in two sorted lists and log differences.

    :param main_sorted: List of documents from the main database state
    :type main_sorted: List[Document]
    :param branch_sorted: List of documents from the branch database state
    :type branch_sorted: List[Document]
    :return: True if differences are found, False otherwise
    :rtype: bool
    """
    differences_found = False
    differences = {"differences": {}}

    for main_doc, branch_doc in zip(main_sorted, branch_sorted):
        if main_doc.import_id != branch_doc.import_id:
            logger.info(
                f"❌ Import ID difference found {main_doc.import_id} "
                f"vs {branch_doc.import_id}"
            )

        if main_doc != branch_doc:
            doc_differences = {}
            for field in main_doc.model_fields_set:
                main_value = getattr(main_doc, field)
                branch_value = getattr(branch_doc, field)

                # Special handling for 'languages' and 'geographies' fields
                if field in ["languages", "geographies"]:
                    main_value = sorted(main_value)
                    branch_value = sorted(branch_value)

                if field == "publication_ts":
                    main_value = cast(datetime, main_value).isoformat()
                    branch_value = cast(datetime, branch_value).isoformat()

                if main_value != branch_value:
                    doc_differences[field] = {
                        "main": main_value,
                        "branch": branch_value,
                    }
                    logger.info(
                        f"🔍 Field '{field}' differs in document '{main_doc.import_id}': "
                        f"main '{main_value}' vs branch '{branch_value}'"
                    )
            if doc_differences:
                differences["differences"][main_doc.import_id] = doc_differences
                differences_found = True

    # Write differences to a JSON file
    if differences_found:
        with open("document_differences.json", "w") as json_file:
            json.dump(differences, json_file, indent=4)

    return differences_found


def compare_db_states(main_db: InputData, branch_db: InputData):
    """Compare two DB state files and log differences.

    :param main_db: The main database state (source of truth)
    :type main_db: DBState
    :param branch_db: The branch database state under test
    :type branch_db: DBState
    :raises SystemExit: If there are differences in the document lengths
        or contents
    """
    # Sort documents by import_id for order-insensitive comparison
    main_sorted = sorted(main_db.documents.values(), key=lambda doc: doc.import_id)
    branch_sorted = sorted(branch_db.documents.values(), key=lambda doc: doc.import_id)

    if len(main_sorted) != len(branch_sorted):
        logger.info(
            f"🔍 Document list lengths differ: main {len(main_sorted)}, "
            f"branch {len(branch_sorted)}"
        )
        sys.exit(1)

    if find_differing_doc_import_ids(main_sorted, branch_sorted):
        sys.exit(1)

    if find_document_differences(main_sorted, branch_sorted):
        sys.exit(1)

    logger.info("🎉 DB states are equivalent!")


@click.command()
@click.argument("main_db_file", type=click.Path(exists=True))
@click.argument("branch_db_file", type=click.Path(exists=True))
def main(main_db_file: str, branch_db_file: str):
    """Main function to load and compare database states.

    :param main_db_file: Path to the main database state file
    :param branch_db_file: Path to the branch database state file
    """
    main_db_state = load_json(main_db_file)
    branch_db_state = load_json(branch_db_file)

    compare_db_states(main_db_state, branch_db_state)


if __name__ == "__main__":
    main()
