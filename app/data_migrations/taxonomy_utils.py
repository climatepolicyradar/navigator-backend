import json
from typing import Sequence

"""At the moment taxonomy is kept simple, and only supports string validation for enums

For Example:

{
    "topic": {
       allowed_values: [],
       allow_blanks: false,
    },
    ...
}

These functions allow you to reference the values within the json.
See sector_data.json for example each element in the array contains an object where 
we use the "node.name" as the taxonomy values:

  {
    "node": {
      "name": "Energy",
      "description": "Energy",
      "source": "CCLW"
    },
    "children": []
  },

This is referenced in the "file_key_path" as the values to be used when a file is 
loaded:

    {
        "key": "sector",
        "filename": "app/data_migrations/data/cclw/sector_data.json",
        "file_key_path": "node.name",
        "allow_blanks": True,
    },

"""


def dot_dref(obj: dict, dotted_key: str):
    if "." not in dotted_key:
        return obj[dotted_key]
    keys = dotted_key.split(".", 1)
    return dot_dref(obj[keys[0]], keys[1])


def load_metadata_type(filename: str, key_path: str) -> Sequence[str]:
    with open(filename) as file:
        data = json.load(file)
    return [dot_dref(obj, key_path) for obj in data]
