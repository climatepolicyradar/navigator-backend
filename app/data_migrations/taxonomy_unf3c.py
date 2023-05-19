from app.data_migrations.taxonomy_utils import read_taxonomy_values


TAXONOMY_DATA = [
    {
        "key": "author_type",
        "allow_blanks": False,
        "allowed_values": ["Party", "Non-Party"],
    },
]


def get_unf3c_taxonomy():
    return read_taxonomy_values(TAXONOMY_DATA)
