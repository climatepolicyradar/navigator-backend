import re
import sys
from typing import List

# Regular expression pattern to extract the document import ID
DOCUMENT_IMPORT_ID_PATTERN = r'"document_import_id":"([A-Za-z0-9._-]+)"'


def extract_document_ids_from_file(
    file_path: str, pattern: str = DOCUMENT_IMPORT_ID_PATTERN
) -> List[str]:
    """Extracts document import IDs from a JSONL file.

    Args:
        file_path (str): The path to the JSONL file.
        pattern (str): The regular expression pattern to match document IDs.

    Returns:
        List[str]: A list of extracted document import IDs.
    """
    try:
        with open(file_path, "r") as file:
            return [match for line in file for match in re.findall(pattern, line)]
    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found.")
        return []
    except IOError:
        print(f"Error: An error occurred while reading the file '{file_path}'.")
        return []


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python script_name.py <file_path>")
        sys.exit(1)

    input_file_path = sys.argv[1]
    document_ids = extract_document_ids_from_file(input_file_path)
    if document_ids:
        for document_id in document_ids:
            print(document_id)
    else:
        print("No document import IDs found in the file.")
