import json
import os
from pathlib import Path
from typing import Any, Dict

import pytest


@pytest.fixture
def documents_large() -> list[Dict[str, Any]]:
    documents_dir = Path(__file__).parent / "fixtures"

    documents = []

    for file_name in os.listdir(documents_dir):
        file_path = documents_dir / file_name

        with open(file_path, "r") as f:
            documents.append(json.load(f))

    return documents
