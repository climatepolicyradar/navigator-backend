import os
import re

PIPELINE_BUCKET = os.environ["PIPELINE_BUCKET"]
_ID_ELEMENT = r"[a-zA-Z0-9]+([-_]?[a-zA-Z0-9]+)*"
IMPORT_ID_MATCHER = re.compile(
    rf"^{_ID_ELEMENT}\.{_ID_ELEMENT}\.{_ID_ELEMENT}\.{_ID_ELEMENT}$"
)
