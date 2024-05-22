import re

_ID_ELEMENT = r"[a-zA-Z0-9]+([-_]?[a-zA-Z0-9]+)*"
IMPORT_ID_MATCHER = re.compile(
    rf"^{_ID_ELEMENT}\.{_ID_ELEMENT}\.{_ID_ELEMENT}\.{_ID_ELEMENT}$"
)
